{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PackageImports, RankNTypes #-}
-- | A redis backend for persistent.
module Database.Persist.MongoDB
    ( MongoDBReader
    , withMongoDBConn
    , runMongoDBConn 
    , HostName
    , PersistValue(..)
    , keyToDbOid, dbOidToKey
    , module Database.Persist
    ) where

import Database.Persist
import Database.Persist.Base
import Control.Monad.Trans.Reader
import qualified Control.Monad.IO.Class as Trans
import qualified Database.MongoDB as DB
import Control.Applicative (Applicative)
import Control.Exception (toException)
import Data.UString (u)
import qualified Data.CompactString.UTF8 as CS
import Data.Enumerator hiding (map, length)
import Network.Socket (HostName)
import qualified Network.Abstract(NetworkIO)
import Data.Maybe (catMaybes, fromJust)
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import qualified Data.Serialize as S
import qualified Data.ByteString as B
import Debug.Trace

newtype MongoDBReader t m a = MongoDBReader (ReaderT ((DB.ConnPool t), HostName) m a)
    deriving (Monad, Trans.MonadIO, Functor, Applicative)

withMongoDBConn :: (Network.Abstract.NetworkIO m) => t -> HostName -> ((DB.ConnPool DB.Host, t) -> m b) -> m b
withMongoDBConn dbname hostname connectionReader = do
  pool <- DB.newConnPool 1 $ DB.host hostname
  connectionReader (pool, dbname)

runMongoDBConn :: MongoDBReader t m a -> (DB.ConnPool t, HostName) -> m a
runMongoDBConn (MongoDBReader r) conn = do runReaderT r conn

runPool :: (DB.Service s, Trans.MonadIO m) => DB.ConnPool s -> String 
     -> ReaderT DB.Database (DB.Action m) a -> m (Either DB.Failure a)
runPool pool dbname action =
  DB.access DB.safe DB.Master pool $ DB.use (DB.Database (u dbname)) action

execute :: (DB.Service s, Trans.MonadIO m) =>
     ReaderT DB.Database (DB.Action (MongoDBReader s m)) b -> MongoDBReader s m b
execute action = do
  (pool, dbname) <- MongoDBReader ask
  Right result <- runPool pool dbname action
  return result

value :: DB.Field -> DB.Value
value (_ DB.:= val) = val

rightPersistVals :: (PersistEntity val) => [DB.Value] -> (String -> String) -> val
rightPersistVals vals err = case fromPersistValues $ catMaybes (map DB.cast' (tail vals)) of
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
  updateValues = map (DB.val . persistUpdateToValue)

uniqSelector :: forall val.  (PersistEntity val) => Unique val -> [DB.Field]
uniqSelector uniq = zipWith (DB.:=)
  (map u (persistUniqueToFieldNames uniq))
  (map DB.val (persistUniqueToValues uniq))

pairFromDocument :: forall val val1.  (PersistEntity val, PersistEntity val1) => [DB.Field] -> Either String (Key val, val1)
pairFromDocument document = pairFromPersistValues vals 
  where
    pairFromPersistValues (x:xs) =
        case fromPersistValues xs of
            Left e -> Left e
            Right xs' -> Right (toPersistKey x, xs')
    pairFromPersistValues _ = Left "error in fromPersistValues'"
    vals = catMaybes (map (DB.cast' . value) document)

insertFields :: forall val.  (PersistEntity val) => EntityDef -> val -> [DB.Field]
insertFields t record = zipWith (DB.:=) (toLabels) (toValues)
  where
    toLabels = map (u . fst3) $ entityColumns t
    toValues = map (DB.val . toPersistValue) (toPersistFields record)

instance (DB.DbAccess m, DB.Service t) => PersistBackend (MongoDBReader t m) where
    insert record = do
        (DB.ObjId key) <- execute $ DB.insert (u $ entityName t) (insertFields t record)
        return $ toPersistKey $dbOidToKey key 
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
            d <- execute $ DB.findOne (selectByKey k t)
            case d of 
              Nothing -> return Nothing
              Just doc -> do 
                let record = rightPersistVals (map value doc) (\e -> "get " ++ (show d) ++ ": " ++ e)
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

    selectEnum filts ords limit offset = do
      Iteratee . start
      where
        start x = do
            cursor <- execute $ DB.find query
            loop x cursor

        query = (DB.select (filterToSelector filts) (u $ entityName t)) {
          DB.limit = fromIntegral limit
        , DB.skip  = fromIntegral offset
        , DB.sort  = if null ords then [] else map orderClause ords
        }

        t = entityDef $ dummyFromFilts filts
        orderClause o = (u(persistOrderToFieldName o))
                        DB.=: (case persistOrderToOrder o of
                                Asc -> 1 :: Int
                                Desc -> -1 )

        loop (Continue k) curs = do
            doc <- execute $ DB.next curs
            case doc of
                Nothing -> return $ Continue k
                Just document -> case pairFromDocument document of
                        Left s -> return $ Error $ toException
                                $ PersistMarshalException s
                        Right row -> do
                            step <- runIteratee $ k $ Chunks [row]
                            loop step curs
        loop step _ = return step

    selectKeys filts =
        Iteratee . start
      where
        start x = do
            cursor <- execute $ DB.find query
            loop x cursor

        loop (Continue k) curs = do
            doc <- execute $ DB.next curs
            case doc of
                Nothing -> return $ Continue k
                Just [_ DB.:= (DB.ObjId i)] -> do
                    step <- runIteratee $ k $ Chunks [toPersistKey $ dbOidToKey i]
                    loop step curs
                Just y -> return $ Error $ toException $ PersistMarshalException
                        $ "Unexpected in selectKeys: " ++ show y
        loop step _ = return step

        query = (DB.select (filterToSelector filts) (u $ entityName t)) {
          DB.project = [u"_id" DB.=: (1 :: Int)]
        }
        t = entityDef $ dummyFromFilts filts

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
      Left v -> DB.val v
      Right vs -> DB.Array (map DB.val vs)

    showFilter Ne = "$ne"
    showFilter Gt = "$gt"
    showFilter Lt = "$lt"
    showFilter Ge = "$gte"
    showFilter Le = "$lte"
    showFilter In = "$in"
    showFilter NotIn = "$nin"
    showFilter Eq = error ""

mapFromDoc :: DB.Document -> [(T.Text, PersistValue)]
mapFromDoc = Prelude.map (\f -> ( ( csToT (DB.label f)), (fromJust . DB.cast') (DB.value f) ) )

csToT :: CS.CompactString -> T.Text
csToT = E.decodeUtf8 . CS.toByteString

tToCS :: T.Text -> CS.CompactString
tToCS = CS.fromByteString_ . E.encodeUtf8

dbOidToKey :: DB.ObjectId -> PersistValue
dbOidToKey =  PersistByteString . S.encode

keyToDbOid :: B.ByteString -> DB.ObjectId
keyToDbOid k = case S.decode k of
                  Left s -> error s
                  Right o -> o

instance DB.Val PersistValue where
  val (PersistInt64 x)   = DB.Int64 x
  val (PersistText x)    = DB.String (tToCS x)
  val (PersistDouble x)  = DB.Float x
  val (PersistBool x)    = DB.Bool x
  val (PersistUTCTime x) = DB.UTC x
  val (PersistNull)      = DB.Null
  val (PersistList l)    = DB.Array (map DB.val l)
  val (PersistMap  m)    = DB.Doc $ Prelude.map (\(k, v)-> (DB.=:) (tToCS k) v) m
  val (PersistByteString x) = DB.ObjId $ keyToDbOid x
  cast' (DB.Float x) = Just (PersistDouble x)
  cast' (DB.Int32 x)  = Just $ PersistInt64 $ fromIntegral x
  cast' (DB.Int64 x)  = Just $ PersistInt64 x
  cast' (DB.String x) = Just $ PersistText (csToT x) 
  cast' (DB.Bool x)   = Just $ PersistBool x
  cast' (DB.UTC d)    = Just $ PersistUTCTime d
  cast' DB.Null       = Just $ PersistNull
  cast' (DB.Bin (DB.Binary b))   = Just $ PersistByteString b
  cast' (DB.Fun (DB.Function f)) = Just $ PersistByteString f
  cast' (DB.Uuid (DB.UUID uid))  = Just $ PersistByteString uid
  cast' (DB.Md5 (DB.MD5 md5))    = Just $ PersistByteString md5
  cast' (DB.UserDef (DB.UserDefined bs)) = Just $ PersistByteString bs
  cast' (DB.RegEx (DB.Regex us1 us2))    = Just $ PersistByteString $ CS.toByteString $ CS.append us1 us2
  cast' (DB.Doc doc)  = Just $ PersistMap $ mapFromDoc doc
  cast' (DB.Array xs) = Just $ PersistList $ catMaybes (map DB.cast' xs)
  cast' (DB.ObjId x) = Just $ dbOidToKey x 
  cast' _ = Nothing
  -- val (PersistByteString bs) = DB.String $ CS.fromByteString bs
  -- cast' (DB.JavaScr (DB.Javascript (Document doc) (us))) =
  --val (PersistDay d) = H.SqlLocalDate d
  --val (PersistTimeOfDay t) = H.SqlLocalTimeOfDay t

instance S.Serialize DB.ObjectId where
  put (DB.Oid w1 w2) = do S.put w1
                          S.put w2

  get = do w1 <- S.get
           w2 <- S.get
           return (DB.Oid w1 w2) 

dummyFromKey :: Key v -> v
dummyFromKey _ = error "dummyFromKey"
dummyFromUnique :: Unique v -> v
dummyFromUnique _ = error "dummyFromUnique"
dummyFromFilts :: [Filter v] -> v
dummyFromFilts _ = error "dummyFromFilts"
