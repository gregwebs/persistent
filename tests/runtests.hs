{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NoMonomorphismRestriction #-}

import Test.Framework (defaultMain, testGroup, Test)
import Test.Framework.Providers.HUnit
-- import Test.Framework.Providers.QuickCheck
import Test.HUnit hiding (Test)

import Database.Persist.Base (PersistField(..))
import Database.Persist.Sqlite hiding (mkPersist, mkMigrate)
import Database.Persist.Postgresql hiding (mkPersist, persist)
import Database.Persist.MongoDB hiding (mkPersist)
import Database.Persist.TH (share2, derivePersistField, mkPersist)
import Control.Monad.IO.Class

import Control.Monad.Trans.Reader
import Monad (unless)
import Data.Int
import Data.Word

import Control.Exception (SomeException)
import qualified Control.Exception.Peel as Peel

-- these are like HUnit assertions, but with liftIO
infix 1 @==, ==@
expected @== actual = liftIO $ expected @?= actual
expected ==@ actual = liftIO $ expected @=? actual

-- add convenience not equal assertions
infix 1 /=@, @/=

(/=@) :: (Eq a, Show a, MonadIO m) => a -> a -> m ()
expected /=@ actual = liftIO $ assertNotEqual "" expected actual

(@/=) :: (Eq a, Show a, MonadIO m) => a -> a -> m ()
actual @/= expected = liftIO $ assertNotEqual "" expected actual

assertNotEqual :: (Eq a, Show a) => String -> a -> a -> Assertion
assertNotEqual preface expected actual =
  unless (actual /= expected) (assertFailure msg)
  where msg = (if null preface then "" else preface ++ "\n") ++
             "expected: " ++ show expected ++ "\n to not equal: " ++ show actual


data PetType = Cat | Dog
    deriving (Show, Read, Eq)
derivePersistField "PetType"

  -- FIXME Empty
share2 mkPersist (mkMigrate "testMigrate") [$persist|

  Person
    name String update Eq Ne Desc
    age Int update "Asc" Desc Lt "some ignored -- attribute" Eq Add
    color String Maybe Eq Ne -- this is a comment sql=foobarbaz
    PersonNameKey name -- this is a comment sql=foobarbaz
  Pet
    owner PersonId
    name String
    type PetType
  NeedsPet
    pet PetId
  Number
    int Int
    int32 Int32
    word32 Word32
    int64 Int64
    word64 Word64
|]

runConn :: MongoDBReader Host IO () -> IO ()
runConn f = do
    -- (withSqlitePool "testdb" 1) $ runSqlPool f
--    (withPostgresqlPool "user=test password=test host=localhost port=5432 dbname=yesod_test" 1) $ runSqlPool f
    withMongoDBConn "yesod_test" "127.0.0.1" $ runMongoDBConn f

-- sqliteTest :: SqlPersist IO () -> Assertion
sqliteTest :: MongoDBReader Host IO () -> IO ()
sqliteTest actions = do
  runConn actions
  -- runConn cleanDB

cleanDB = do
  deleteWhere ([] :: [Filter Pet])
  deleteWhere ([] :: [Filter Person])
  deleteWhere ([] :: [Filter Number])

-- setup :: SqlPersist IO ()
setup = do
  runMigration testMigrate
  -- cleanDB

main :: IO ()
main = do
  -- runConn setup
  defaultMain [testSuite]

testSuite :: Test
testSuite = testGroup "Database.Persistent"
    [
      testCase "sqlite persistent" case_sqlitePersistent
    , testCase "sqlite deleteWhere" case_sqliteDeleteWhere
    , testCase "sqlite deleteBy" case_sqliteDeleteBy
    , testCase "sqlite delete" case_sqliteDelete
    , testCase "sqlite replace" case_sqliteReplace
    , testCase "sqlite getBy" case_sqliteGetBy
    , testCase "sqlite update" case_sqliteUpdate
    , testCase "sqlite updateWhere" case_sqliteUpdateWhere
    , testCase "sqlite selectList" case_sqliteSelectList
    , testCase "large numbers" case_largeNumbers
    , testCase "insertBy" case_insertBy
    , testCase "derivePersistField" case_derivePersistField
    , testCase "afterException" case_afterException
    ]

                          
assertEmpty xs    = liftIO $ assertBool "" (null xs)
assertNotEmpty xs = liftIO $ assertBool "" (not (null xs))

case_sqlitePersistent = sqliteTest _persistent
case_sqliteDeleteWhere = sqliteTest _deleteWhere
case_sqliteDeleteBy = sqliteTest _deleteBy
case_sqliteDelete = sqliteTest _delete
case_sqliteReplace = sqliteTest _replace
case_sqliteGetBy = sqliteTest _getBy
case_sqliteUpdate = sqliteTest _update
case_sqliteUpdateWhere = sqliteTest _updateWhere
case_sqliteSelectList = sqliteTest _selectList
case_largeNumbers = sqliteTest _largeNumbers
case_insertBy = sqliteTest _insertBy
case_derivePersistField = sqliteTest _derivePersistField
case_afterException = sqliteTest _afterException

_deleteWhere :: MongoDBReader Host IO ()
_deleteWhere = do
  key2 <- insert $ Person "Michael2" 90 Nothing
  _    <- insert $ Person "Michael3" 90 Nothing
  let p91 = Person "Michael4" 91 Nothing
  key91 <- insert $ p91

  ps90 <- selectList [PersonAgeEq 90] [] 0 0
  assertNotEmpty ps90
  deleteWhere [PersonAgeEq 90]
  ps90 <- selectList [PersonAgeEq 90] [] 0 0
  assertEmpty ps90
  Nothing <- get key2

  Just p2_91 <- get key91
  p91 @== p2_91

_deleteBy :: MongoDBReader Host IO ()
_deleteBy = do
  key2 <- insert $ Person "Michael2" 27 Nothing
  let p3 = Person "Michael3" 27 Nothing
  key3 <- insert $ p3

  ps2 <- selectList [PersonNameEq "Michael2"] [] 0 0
  assertNotEmpty ps2

  deleteBy $ PersonNameKey "Michael2"
  ps2 <- selectList [PersonNameEq "Michael2"] [] 0 0
  assertEmpty ps2

  Just p32 <- get key3
  p3 @== p32

_delete :: MongoDBReader Host IO ()
_delete = do
  key2 <- insert $ Person "Michael2" 27 Nothing
  let p3 = Person "Michael3" 27 Nothing
  key3 <- insert $ p3

  pm2 <- selectList [PersonNameEq "Michael2"] [] 0 0
  assertNotEmpty pm2
  delete key2
  pm2 <- selectList [PersonNameEq "Michael2"] [] 0 0
  assertEmpty pm2

  Just p <- get key3
  p3 @== p

-- also a decent test of get
_replace :: MongoDBReader Host IO ()
_replace = do
  key2 <- insert $ Person "Michael2" 27 Nothing
  let p3 = Person "Michael3" 27 Nothing
  replace key2 p3
  Just p <- get key2
  p @== p3

  -- test replace an empty key
  delete key2
  Nothing <- get key2
  undefined <- replace key2 p3
  Nothing <- get key2
  return ()

_getBy :: MongoDBReader Host IO ()
_getBy = do
  let p2 = Person "Michael2" 27 Nothing
  key2 <- insert p2
  Just (k, p) <- getBy $ PersonNameKey "Michael2"
  p @== p2
  k @== key2
  Nothing <- getBy $ PersonNameKey "Michael3"
  return ()

_update :: MongoDBReader Host IO ()
_update = do
  let p25 = Person "Michael" 25 Nothing
  key25 <- insert p25
  update key25 [PersonAge 28, PersonName "Updated"]
  Just pBlue28 <- get key25
  pBlue28 @== Person "Updated" 28 Nothing
  update key25 [PersonAgeAdd 2]
  Just pBlue30 <- get key25
  pBlue30 @== Person "Updated" 30 Nothing

_updateWhere :: MongoDBReader Host IO ()
_updateWhere = do
  let p1 = Person "Michael" 25 Nothing
  let p2 = Person "Michael2" 25 Nothing
  key1 <- insert p1
  key2 <- insert p2
  updateWhere [PersonNameEq "Michael2"]
              [PersonAgeAdd 3, PersonName "Updated"]
  Just pBlue28 <- get key2
  pBlue28 @== Person "Updated" 28 Nothing
  Just p <- get key1
  p @== p1

_selectList :: MongoDBReader Host IO ()
_selectList = do
  let p25 = Person "Michael" 25 Nothing
  let p26 = Person "Michael2" 26 Nothing
  key25 <- insert p25
  key26 <- insert p26
  ps <- selectList [] [] 0 0
  ps @== [(key25, p25), (key26, p26)]
  -- limit
  ps <- selectList [] [] 1 0
  ps @== [(key25, p25)]
  -- offset -- FAILS!
  ps <- selectList [] [] 0 1
  ps @== [(key26, p26)]
  -- limit & offset
  ps <- selectList [] [] 1 1
  ps @== [(key26, p26)]

  ps <- selectList [] [PersonAgeDesc] 0 0
  ps @== [(key26, p26), (key25, p25)]
  ps <- selectList [PersonAgeEq 26] [] 0 0
  ps @== [(key26, p26)]

-- general tests transferred from already exising test file
_persistent :: MongoDBReader Host IO ()
_persistent = do
  let mic = Person "Michael" 25 Nothing
  micK <- insert mic
  Just p <- get micK
  p @== mic

  replace micK $ Person "Michael" 25 Nothing
  Just p <- get micK
  p @== mic

  replace micK $ Person "Michael" 26 Nothing
  Just mic26 <- get micK
  mic26 @/= mic
  personAge mic26 @== (personAge mic) + 1

  results <- selectList [PersonNameEq "Michael"] [] 0 0
  results @== [(micK, mic26)]

  results <- selectList [PersonAgeLt 28] [] 0 0
  results @== [(micK, mic26)]

  update micK [PersonAge 28]
  Just p28 <- get micK
  personAge p28 @== 28

  updateWhere [PersonNameEq "Michael"] [PersonAge 29]
  Just mic29 <- get micK
  personAge mic29 @== 29

  let eli = Person "Eliezer" 2 $ Just "blue"
  _ <- insert eli
  pasc <- selectList [] [PersonAgeAsc] 0 0
  (map snd pasc) @== [eli, mic29]

  let abe30 = Person "Abe" 30 $ Just "black"
  keyAbe30 <- insert abe30
  pdesc <- selectList [PersonAgeLt 30] [PersonNameDesc] 0 0
  (map snd pasc) @== [eli, mic29]

  abes <- selectList [PersonNameEq "Abe"] [] 0 0
  (map snd abes) @== [abe30]

  Just (k,p) <- getBy $ PersonNameKey "Michael"
  p @== mic29

  ps <- selectList [PersonColorEq $ Just "blue"] [] 0 0
  map snd ps @== [eli]

  ps <- selectList [PersonColorEq Nothing] [] 0 0
  map snd ps @== [mic29]

  ps <- selectList [PersonColorNe Nothing] [] 0 0
  map snd ps @== [eli, abe30]

  delete micK
  Nothing <- get micK
  return ()

_largeNumbers :: MongoDBReader Host IO ()
_largeNumbers = do
    go $ Number maxBound 0 0 0 0
    go $ Number 0 maxBound 0 0 0
    go $ Number 0 0 maxBound 0 0
    go $ Number 0 0 0 maxBound 0
    go $ Number 0 0 0 0 maxBound

    go $ Number minBound 0 0 0 0
    go $ Number 0 minBound 0 0 0
    go $ Number 0 0 minBound 0 0
    go $ Number 0 0 0 minBound 0
    go $ Number 0 0 0 0 minBound
  where
    go x = do
        xid <- insert x
        x' <- get xid
        liftIO $ x' @?= Just x

_insertBy :: MongoDBReader Host IO ()
_insertBy = do
    Right _ <- insertBy $ Person "name" 1 Nothing
    Left _ <- insertBy $ Person "name" 1 Nothing
    Right _ <- insertBy $ Person "name2" 1 Nothing
    return ()

_derivePersistField :: MongoDBReader Host IO ()
_derivePersistField = do
    person <- insert $ Person "pet owner" 30 Nothing
    cat <- insert $ Pet person "Mittens" Cat
    Just cat' <- get cat
    liftIO $ petType cat' @?= Cat
    dog <- insert $ Pet person "Spike" Dog
    Just dog' <- get dog
    liftIO $ petType dog' @?= Dog

_afterException = do
    _ <- insert $ Person "A" 0 Nothing
    _ <- (insert (Person "A" 1 Nothing) >> return ()) `Peel.catch` catcher
    _ <- insert $ Person "B" 0 Nothing
    return ()
  where
    catcher :: Monad m => SomeException -> m ()
    catcher _ = return ()
