{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PackageImports, RankNTypes #-}
-- | A redis backend for persistent.
module Database.Persist.MongoDB
    ( MongoDBReader
    , Connection
    , module Database.Persist
    ) where

import Database.Persist
import Database.Persist.Base
-- import Database.Persist.Pool
import Control.Monad.Trans.Reader
import qualified Control.Monad.IO.Class as Trans
import Control.Monad.Trans.Class (MonadTrans (..))
import "MonadCatchIO-transformers" Control.Monad.CatchIO
import "mtl" Control.Monad.Trans (MonadIO (..))
import qualified Database.MongoDB as DB
import Control.Applicative (Applicative)
-- import Control.Monad (forM_, forM)
-- import qualified Data.ByteString.UTF8 as SU
import Data.UString (u)
import qualified Data.CompactString.UTF8 as CS
import Data.Enumerator hiding (map, length)
-- import Data.Maybe (fromMaybe, mapMaybe, fromJust)

type Connection = DB.Connection

-- | A ReaderT monad transformer holding a mongoDB database connection.
newtype MongoDBReader m a = MongoDBReader (ReaderT Connection m a)
    deriving (Monad, Trans.MonadIO, MonadTrans, MonadCatchIO, Functor,
              Applicative)

instance Trans.MonadIO m => MonadIO (MongoDBReader m) where
    liftIO = Trans.liftIO

runConn conn action =
  DB.runNet $ (flip DB.runConn) conn (DB.useDb (u"test") action)

execute action = do
  conn <- MongoDBReader ask
  Right (Right result) <- runConn conn action
  return result

value :: DB.Field -> DB.Value
value (_ DB.:= val) = val

rightPersistVals :: (PersistEntity val) => [DB.Value] -> (String -> String) -> val
rightPersistVals vals err = case fromPersistValues (map pFromB vals) of
    Left e -> error (err e)
    Right v -> v

fst3 :: forall t t1 t2. (t, t1, t2) -> t
fst3 (x, _, _) = x

filterByKey :: (PersistEntity val) => Key val -> DB.Document
filterByKey k = [u"_id" DB.=: fromPersistKey k]

selectByKey :: forall val aQueryOrSelection.  (PersistEntity val, DB.Select aQueryOrSelection) => Key val -> EntityDef -> aQueryOrSelection
selectByKey k entity = DB.select (filterByKey k) (u $ entityName entity)

updateField :: (PersistEntity val) => [Update val] -> DB.Field
updateField upds = u"$set" DB.=: (
    zipWith (DB.:=) (updateLabels upds) (updateValues upds)
  ) 
  where
  updateLabels = map (u . persistUpdateToFieldName)
  updateValues = map (pToB . persistUpdateToValue)

uniqSelector :: forall val.  (PersistEntity val) => Unique val -> [DB.Field]
uniqSelector uniq = zipWith (DB.:=)
  (map u (persistUniqueToFieldNames uniq))
  (map pToB (persistUniqueToValues uniq))

pairFromDocument :: forall val val1.  (PersistEntity val, PersistEntity val1) => [DB.Field] -> Either String (Key val, val1)
pairFromDocument document = pairFromPersistValues $ map pFromB (map value document)
  where
    pairFromPersistValues (PersistInt64 x:xs) =
        case fromPersistValues xs of
            Left e -> Left e
            Right xs' -> Right (toPersistKey x, xs')
    pairFromPersistValues _ = Left "error in fromPersistValues'"

insertFields :: forall val.  (PersistEntity val) => EntityDef -> val -> [DB.Field]
insertFields t record = zipWith (DB.:=) (toLabels) (toValues)
  where
    toLabels = map (u . fst3) $ entityColumns t
    toValues = map (pToB . toPersistValue) (toPersistFields record)

instance Trans.MonadIO m => PersistBackend (MongoDBReader m) where
    insert record = do
        DB.Int64 key <- execute $ DB.insert (u $ entityName t) (insertFields t record)
        return $ toPersistKey (key)
      where
        t = entityDef record

    replace k record = do
        execute $ DB.replace (selectByKey k t) (insertFields t record)
        return ()
      where
        t = entityDef record

    update _ [] = return ()
    update k upds = do
        execute $ DB.save (u $ entityName t)
          [u"_id" DB.=: (fromPersistKey k), updateField upds] 
      where
        t = entityDef $ dummyFromKey k

    updateWhere _ [] = return ()
    updateWhere filts upds = do
        execute $ DB.modify DB.Select {
          DB.coll = (u $ entityName t)
        , DB.selector = filterToSelector filts
        } [updateField upds]
      where
        t = entityDef $ dummyFromFilts filts

    delete k =
        execute $ DB.deleteOne DB.Select {
          DB.coll = (u $ entityName t)
        , DB.selector = filterByKey k
        }
      where
        t = entityDef $ dummyFromKey k

    deleteWhere filts = do
        execute $ DB.delete DB.Select {
          DB.coll = (u $ entityName t)
        , DB.selector = filterToSelector filts
        }
      where
        t = entityDef $ dummyFromFilts filts

    deleteBy uniq =
        execute $ DB.delete DB.Select {
          DB.coll = u $ entityName t
        , DB.selector = uniqSelector uniq
        }
      where
        t = entityDef $ dummyFromUnique uniq

    get k = do
        Just doc <- execute $ DB.findOne (selectByKey k t) 
        let record = rightPersistVals (map value doc) (\e -> "get " ++ showPersistKey k ++ ": " ++ e)
        return $ Just record
      where
        t = entityDef $ dummyFromKey k

    getBy uniq = do
        mdocument <- execute $ DB.findOne $
          DB.select (uniqSelector uniq) (u $ entityName t)
        case mdocument of
          Nothing -> return Nothing
          Just document -> case pairFromDocument document of
              Left s -> error s
              Right (k, x) -> return $ Just (k, x)
      where
        t = entityDef $ dummyFromUnique uniq

    count filts = do
        i <- execute $ DB.count query
        return $ fromIntegral i
      where
        query = DB.select (filterToSelector filts) (u $ entityName t)
        t = entityDef $ dummyFromFilts filts

    select filts ords limit offset = do
      Iteratee . start
      where
        start x = do
            cursor <- execute $ DB.find query
            loop x cursor

        query = DB.Query {
          DB.limit = fromIntegral limit
        , DB.skip  = fromIntegral offset
        , DB.sort  = if null ords then [] else map orderClause ords
        , DB.selection = DB.Select (filterToSelector filts) $ u(entityName t)
        }

        t = entityDef $ dummyFromFilts filts
        orderClause o = (u(persistOrderToFieldName o))
                        DB.=: (case persistOrderToOrder o of
                                Asc -> 1
                                Desc -> -1)
        loop (Continue k) curs = do
            doc <- DB.next curs
            case doc of
                Nothing -> return $ Continue k
                Just document -> case pairFromDocument document of
                        Left s -> return $ Error $ toException
                                $ PersistMarshalException s
                        Right row -> do
                            step <- runIteratee $ k $ Chunks [row]
                            loop step curs
        loop step _ = return step


filterToSelector :: PersistEntity val => [Filter val] -> DB.Document
filterToSelector filts = map filterField filts

filterField :: PersistEntity val => Filter val -> DB.Field
filterField f = case filt of
    Eq -> name DB.:= filterValue
    _  -> name DB.=: [u(showFilter filt) DB.:= filterValue]
  where
    name = u $ persistFilterToFieldName f
    filt = persistFilterToFilter f
    filterValue = case persistFilterToValue f of
      Left v -> pToB v
      Right vs -> DB.Array (map pToB vs)

    showFilter Ne = "$ne"
    showFilter Gt = "$gt"
    showFilter Lt = "$lt"
    showFilter Ge = "$gte"
    showFilter Le = "$lte"
    showFilter In = "$in"
    showFilter NotIn = "$nin"
    showFilter Eq = error ""

pToB :: PersistValue -> DB.Value
pToB (PersistString s) = DB.String $ u s
-- pToB (PersistByteString bs) = DB.String $ CS.fromByteString bs
pToB (PersistInt64 i) = DB.Int64 i
pToB (PersistDouble d) = DB.Float d
pToB (PersistBool b) = DB.Bool b
-- pToSql (PersistDay d) = H.SqlLocalDate d
-- pToSql (PersistTimeOfDay t) = H.SqlLocalTimeOfDay t
pToB (PersistUTCTime t) = DB.UTC t
pToB PersistNull = DB.Null

pFromB :: DB.Value -> PersistValue
pFromB (DB.String us) = PersistByteString $ CS.toByteString us
pFromB (DB.Int32 i) = PersistInt64 $ fromIntegral i
pFromB (DB.Int64 i) = PersistInt64 $ fromIntegral i
pFromB (DB.Bool b) = PersistBool b
pFromB (DB.Float b) = PersistDouble b
pFromB (DB.UTC d) = PersistUTCTime d
pFromB DB.Null = PersistNull
pFromB (DB.Bin (DB.Binary b)) = PersistByteString b
pFromB (DB.Fun (DB.Function f)) = PersistByteString f
pFromB (DB.Uuid (DB.UUID uid)) = PersistByteString uid
pFromB (DB.Md5 (DB.MD5 md5)) = PersistByteString md5
pFromB (DB.UserDef (DB.UserDefined bs)) = PersistByteString bs
pFromB (DB.RegEx (DB.Regex us1 us2)) =
  PersistByteString $ CS.toByteString $ CS.append us1 us2
-- pFromB (DB.Doc doc) = map pFromB
-- pFromB (DB.Array xs) = map pFromB xs
-- pFromB (DB.ObjId (DB.Oid i32 i64)) = PersistInt64 $ fromIntegral i64
-- pFromB (DB.JavaScr (DB.Javascript (Document doc) (us))) =

dummyFromKey :: Key v -> v
dummyFromKey _ = error "dummyFromKey"
dummyFromUnique :: Unique v -> v
dummyFromUnique _ = error "dummyFromUnique"
dummyFromFilts :: [Filter v] -> v
dummyFromFilts _ = error "dummyFromFilts"
