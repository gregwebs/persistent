{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PackageImports, RankNTypes #-}
-- | A redis backend for persistent.
module Database.Persist.MongoDB
    ( MongoDBReader
    , withMongoDBConn
    , runMongoDBConn 
    , HostName
    , PersistValue(..)
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
import Data.Maybe (mapMaybe, fromJust)
import qualified Data.Text as T
import qualified Data.Text.Encoding as E
import qualified Data.Serialize as S

{-
import Debug.Trace
debug :: (Show a) => a -> a
debug a = trace ("DEBUG: " ++ show a) a
-}

newtype MongoDBReader t m a = MongoDBReader (ReaderT ((DB.ConnPool t), HostName) m a)
    deriving (Monad, Trans.MonadIO, Functor, Applicative)

withMongoDBConn :: (Network.Abstract.NetworkIO m) => t -> HostName -> ((DB.ConnPool DB.Host, t) -> m b) -> m b
withMongoDBConn dbname hostname connectionReader = do
  pool <- DB.newConnPool 1 $ DB.host hostname
  connectionReader (pool, dbname)

runMongoDBConn :: MongoDBReader t m a -> (DB.ConnPool t, HostName) -> m a
runMongoDBConn (MongoDBReader r) = runReaderT r 

runPool :: (DB.Service s, Trans.MonadIO m) => DB.ConnPool s -> String 
     -> ReaderT DB.Database (DB.Action m) a -> m (Either DB.Failure a)
runPool pool dbname action =
  DB.access DB.safe DB.Master pool $ DB.use (DB.Database (u dbname)) action

execute :: (DB.Service s, Trans.MonadIO m) =>
     ReaderT DB.Database (DB.Action (MongoDBReader s m)) b -> MongoDBReader s m b
execute action = do
  (pool, dbname) <- MongoDBReader ask
  res <-  runPool pool dbname action
  case res of
      (Right result) -> return result 
      (Left x) -> fail (show x)   -- TODO what to put here?

value :: DB.Field -> DB.Value
value (_ DB.:= val) = val

rightPersistVals :: (PersistEntity val) => EntityDef -> [DB.Field] -> val
rightPersistVals ent vals = case wrapFromPersistValues ent vals of
      Left e -> error e
      Right v -> v

fst3 :: forall t t1 t2. (t, t1, t2) -> t
fst3 (x, _, _) = x

filterByKey :: (PersistEntity val) => Key val -> DB.Document
filterByKey k = [u"_id" DB.=: keyToDbOid k]

queryByKey :: (PersistEntity val) => Key val -> EntityDef -> DB.Query 
queryByKey k entity = (DB.select (filterByKey k) (u $ entityName entity)) 

selectByKey :: (PersistEntity val) => Key val -> EntityDef -> DB.Selection 
selectByKey k entity = (DB.select (filterByKey k) (u $ entityName entity))

updateFields :: (PersistEntity val) => [Update val] -> [DB.Field]
updateFields upds = map updateField upds 

updateField :: (PersistEntity val) => Update val -> DB.Field
updateField upd = opName DB.:= DB.Doc [( (u $ persistUpdateToFieldName upd) DB.:= opValue)]
    where 
      opValue = (DB.val $ transform $ persistUpdateToValue upd)
      transform (PersistInt64 i) = PersistInt64 $
        case persistUpdateToUpdate upd of
          Subtract -> -i
          _        ->  i
      transform x = x
      opName = case persistUpdateToUpdate upd of
                    Update   -> u "$set"
                    Add      -> u "$inc"
                    Subtract -> u "$inc"
                    Multiply -> error "multiply not supported yet"
                    Divide   -> error "divide not supported yet"


uniqSelector :: forall val.  (PersistEntity val) => Unique val -> [DB.Field]
uniqSelector uniq = zipWith (DB.:=)
  (map u (persistUniqueToFieldNames uniq))
  (map DB.val (persistUniqueToValues uniq))

pairFromDocument :: forall val val1.  (PersistEntity val, PersistEntity val1) => EntityDef -> [DB.Field] -> Either String (Key val, val1)
pairFromDocument ent document = pairFromPersistValues document
  where
    pairFromPersistValues (x:xs) =
        case wrapFromPersistValues ent xs of
            Left e -> Left e
            Right xs' -> Right ((toPersistKey . fromJust . DB.cast' . value) x, xs')
    pairFromPersistValues _ = Left "error in fromPersistValues'"

insertFields :: forall val.  (PersistEntity val) => EntityDef -> val -> [DB.Field]
insertFields t record = zipWith (DB.:=) (toLabels) (toValues)
  where
    toLabels = map (u . fst3) $ entityColumns t
    toValues = map (DB.val . toPersistValue) (toPersistFields record)

instance (DB.DbAccess m, DB.Service t) => PersistBackend (MongoDBReader t m) where
    insert record = do
        (DB.ObjId oid) <- execute $ DB.insert (u $ entityName t) (insertFields t record)
        return $ toPersistKey $ dbOidToKey oid 
      where
        t = entityDef record

    replace k record = do
        execute $ DB.replace (selectByKey k t) (insertFields t record)
        return ()
      where
        t = entityDef record

    update _ [] = return ()
    update k upds =
        execute $ DB.modify 
                     (DB.Select [u"_id" DB.:= (DB.ObjId $ keyToDbOid k)]  (u $ entityName t)) 
                     $ updateFields upds
      where
        t = entityDef $ dummyFromKey k

    updateWhere _ [] = return ()
    updateWhere filts upds =
        execute $ DB.modify DB.Select {
          DB.coll = (u $ entityName t)
        , DB.selector = filterToSelector filts
        } $ updateFields upds
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
            d <- execute $ DB.findOne (queryByKey k t)
            case d of
              Nothing -> return Nothing
              Just doc -> do
                return $ Just $ rightPersistVals t (tail doc)
          where
            t = entityDef $ dummyFromKey k

    getBy uniq = do
        mdocument <- execute $ DB.findOne $
          (DB.select (uniqSelector uniq) (u $ entityName t))
        case mdocument of
          Nothing -> return Nothing
          Just document -> case pairFromDocument t document of
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

    selectEnum filts ords limit offset = Iteratee . start
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
                Just document -> case pairFromDocument t document of
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
                Just [_ DB.:= (DB.ObjId oid)] -> do
                    step <- runIteratee $ k $ Chunks [toPersistKey $ dbOidToKey oid]
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
    name = case (persistFilterToFieldName f) of
            "id"  -> u "_id"
            other -> u other
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

wrapFromPersistValues :: (PersistEntity val) => EntityDef -> [DB.Field] -> Either String val
wrapFromPersistValues e doc = fromPersistValues reorder
  where
    castDoc = mapFromDoc doc
    castColumns = map (T.pack . fst3) $ (entityColumns e) 
    -- we have an alist of fields that need to be the same order as entityColumns
    --
    -- this naive lookup is O(n^2)
    -- reorder = map (fromJust . (flip Prelude.lookup $ castDoc)) castColumns
    --
    -- this is O(n * log(n))
    -- reorder =  map (\c -> (M.fromList castDoc) M.! c) castColumns 
    --
    -- and finally, this is O(n * log(n))
    -- * do an alist lookup for each column
    -- * but once we found an item in the alist use a new alist without that item for future lookups
    -- * so for the last query there is only one item left
    reorder :: [PersistValue] 
    reorder = match castColumns castDoc []
      where
        match :: [T.Text] -> [(T.Text, PersistValue)] -> [PersistValue] -> [PersistValue]
        match [] [] values = values
        match (c:cs) fields values =
          let (found, unused) = matchOne fields []
          in match cs unused (values ++ [snd found])
          where
            matchOne (f:fs) tried =
              if c == fst f then (f, tried ++ fs) else matchOne fs (f:tried)
            matchOne fields tried = error $ "field doesn't match" ++ (show c) ++ (show fields) ++ (show tried)
        match cs fields values = error $ "fields don't match" ++ (show cs) ++ (show fields) ++ (show values)

mapFromDoc :: DB.Document -> [(T.Text, PersistValue)]
mapFromDoc = Prelude.map (\f -> ( ( csToT (DB.label f)), (fromJust . DB.cast') (DB.value f) ) )

csToT :: CS.CompactString -> T.Text
csToT = E.decodeUtf8 . CS.toByteString

tToCS :: T.Text -> CS.CompactString
tToCS = CS.fromByteString_ . E.encodeUtf8

dbOidToKey :: DB.ObjectId -> PersistValue
dbOidToKey =  PersistForeignKey . S.encode

foreignKeyToDbOid :: PersistValue -> DB.ObjectId
foreignKeyToDbOid (PersistForeignKey k) = case S.decode k of
                  Left s -> error s
                  Right o -> o
foreignKeyToDbOid _ = error "expected PersistForeignKey"

keyToDbOid :: (PersistEntity val) => Key val -> DB.ObjectId
keyToDbOid = foreignKeyToDbOid . fromPersistKey

instance DB.Val PersistValue where
  val (PersistInt64 x)   = DB.Int64 x
  val (PersistText x)    = DB.String (tToCS x)
  val (PersistDouble x)  = DB.Float x
  val (PersistBool x)    = DB.Bool x
  val (PersistUTCTime x) = DB.UTC x
  val (PersistNull)      = DB.Null
  val (PersistList l)    = DB.Array $ map DB.val l
  val (PersistMap  m)    = DB.Doc $ map (\(k, v)-> (DB.=:) (tToCS k) v) m
  val (PersistByteString x) = DB.String $ CS.fromByteString_ x 
  val x@(PersistForeignKey _) = DB.ObjId $ foreignKeyToDbOid x
  val (PersistDay _)        = error "only PersistUTCTime currently implemented"
  val (PersistTimeOfDay _)  = error "only PersistUTCTime currently implemented"
  cast' (DB.Float x)  = Just (PersistDouble x)
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
  cast' (DB.Array xs) = Just $ PersistList $ mapMaybe DB.cast' xs
  cast' (DB.ObjId x) = Just $ dbOidToKey x 
  cast' (DB.JavaScr _) = error "cast operation not supported for javascript"
  cast' (DB.Sym _) = error "cast operation not supported for sym"
  cast' (DB.Stamp _) = error "cast operation not supported for stamp"
  cast' (DB.MinMax _) = error "cast operation not supported for minmax"

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
