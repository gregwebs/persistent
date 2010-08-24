{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
import Test.Framework (defaultMain, testGroup, Test)
import Test.Framework.Providers.HUnit
import Test.HUnit hiding (Test)

import Database.Persist.Sqlite
import Control.Monad.IO.Class
{-import Data.ByteString.Lazy.UTF8 (toString)-}
{-import Data.List (intercalate)-}

mkPersist [$persist|
Empty

Person
    name String update Eq Ne Desc
    age Int update "Asc" Lt "some ignored attribute"
    color String null Eq Ne
    PersonNameKey name
Pet
    owner PersonId
    name String
|]


connstr :: String
connstr = "user=test password=test host=localhost port=5432 dbname=test"

main :: IO ()
main = do
  withSqlitePool connstr 1 $ runSqlPool setup
  defaultMain $ describe "Database.Persistent" [
      testCase "empty" caseEmpty
    ]

-- describe :: String -> [TestCase] -> Test
describe desc tests = [testGroup desc tests]

testSuite :: Test
testSuite = testGroup "Database.Persistent"
    [ testCase "empty" caseEmpty
    ]


caseEmpty :: Assertion
caseEmpty = Empty {} @=? Empty {}

setup :: SqlPersist IO ()
setup = do
  runMigration $ do
    migrate (undefined :: Person)
    migrate (undefined :: Pet)
  deleteWhere ([] :: [Filter Person])
  deleteWhere ([] :: [Filter Pet])
  pid <- insert $ Person "Michael" 25 Nothing
  liftIO $ print pid

  p1 <- get pid
  liftIO $ print p1

  replace pid $ Person "Michael" 26 Nothing
  p2 <- get pid
  liftIO $ print p2

  p3 <- selectList [PersonNameEq "Michael"] [] 0 0
  liftIO $ print p3

  _ <- insert $ Person "Michael2" 27 Nothing
  deleteWhere [PersonNameEq "Michael2"]
  p4 <- selectList [PersonAgeLt 28] [] 0 0
  liftIO $ print p4

  update pid [PersonAge 28]
  p5 <- get pid
  liftIO $ print p5

  updateWhere [PersonNameEq "Michael"] [PersonAge 29]
  p6 <- get pid
  liftIO $ print p6

  _ <- insert $ Person "Eliezer" 2 $ Just "blue"
  p7 <- selectList [] [PersonAgeAsc] 0 0
  liftIO $ print p7

  _ <- insert $ Person "Abe" 30 $ Just "black"
  p8 <- selectList [PersonAgeLt 30] [PersonNameDesc] 0 0
  liftIO $ print p8

  {-
  insertR $ Person "Abe" 31 $ Just "brown"
  p9 <- select [PersonNameEq "Abe"] []
  liftIO $ print p9
  -}

  p10 <- getBy $ PersonNameKey "Michael"
  liftIO $ print p10

  p11 <- selectList [PersonColorEq $ Just "blue"] [] 0 0
  liftIO $ print p11

  p12 <- selectList [PersonColorEq Nothing] [] 0 0
  liftIO $ print p12

  p13 <- selectList [PersonColorNe Nothing] [] 0 0
  liftIO $ print p13

  delete pid
  plast <- get pid
  liftIO $ print plast

