{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE PackageImports, RankNTypes #-}
-- | A redis backend for persistent.
module Database.Persist.MongoDB
    ( MongoDBReader
    -- , runMongoDB
    -- , withMongoDB
    , Connection
    -- , Pool
    , module Database.Persist
    ) where

import Database.Persist
import Database.Persist.Base
import Database.Persist.Pool
import Control.Monad.Trans.Reader
import qualified Control.Monad.IO.Class as Trans
import Control.Monad.Trans.Class (MonadTrans (..))
import "MonadCatchIO-transformers" Control.Monad.CatchIO
import "mtl" Control.Monad.Trans (MonadIO (..))
import qualified Database.MongoDB as DB
import Control.Applicative (Applicative)
import Control.Monad (forM_, forM)
import qualified Data.ByteString.UTF8 as SU
import Data.UString (u)
import Data.Maybe (fromMaybe, mapMaybe, fromJust)

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
value (label DB.:= val) = val

rightPersistVal :: (PersistEntity val) => [DB.Value] -> (String -> String) -> val
rightPersistVal vals err = case fromPersistValues (map castToPersistValue vals) of
    Left e -> error (err e)
    Right v -> v
  where
    castToPersistValue = toPersistValue . fromJust . DB.cast

selectByKey k = (DB.select [u"_id" DB.=: fromPersistKey k] $ u"test")

fst3 (x, _, _) = x

toDBValue :: PersistValue -> DB.Value
toDBValue x = let Right x' = fromPersistValue x in DB.val x' 

instance Trans.MonadIO m => PersistBackend (MongoDBReader m) where
-- instance MonadCatchIO m => PersistBackend (MongoDBReader m) where
    initialize _ = return ()
    insert record = do
        let name = entityName t
        DB.Int64 key <- execute $ DB.insert (u name) fields
        return $ toPersistKey (key)
      where
        t = entityDef record
        vals = map (toDBValue . toPersistValue) (toPersistFields record)
        columns :: [DB.UString]
        columns = map (u . fst3) $ entityColumns t
        fields :: DB.Document
        fields = zipWith (DB.:=) columns vals

    replace k record = do
        let name = entityName t
        key <- execute $ DB.replace (selectByKey k)  fields
        return ()
      where
        t = entityDef record
        vals = map (toDBValue . toPersistValue) (toPersistFields record)
        columns :: [DB.UString]
        columns = map (u . fst3) $ entityColumns t
        fields :: DB.Document
        fields = zipWith (DB.:=) columns vals

    get k = do
        Just doc <- execute $ DB.findOne (selectByKey k) 
        record <- rightPersistVal (map value doc) (\e -> "get " ++ showPersistKey k ++ ": " ++ e)
        return $ Just record
