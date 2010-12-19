{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
import Prelude hiding (filter)
import Database.Persist.MongoDB
import Control.Monad.IO.Class
import Database.Persist.Pool
import Database.MongoDB

mkPersist [$persist|
Person
    name String update Eq Ne Desc
    age Int update "Asc" Lt "some ignored attribute"
    color String null Eq Ne
    PersonNameKey name
|]

main :: IO ()
main = withRedis localhost defaultPort 1 $ runRedis go

go :: RedisReader IO ()
go = do
    initialize (undefined :: Person)
    pid <- insert $ Person "Michael" 25 Nothing
    liftIO $ print pid

    p1 <- get pid
    liftIO $ print p1

    replace pid $ Person "Michael" 26 Nothing
    p2 <- get pid
    liftIO $ print p2

    p3 <- select [PersonNameEq "Michael"] []
    liftIO $ print p3

    _ <- insert $ Person "Michael2" 27 Nothing
    deleteWhere [PersonNameEq "Michael2"]
    p4 <- select [PersonAgeLt 28] []
    liftIO $ print p4

    update pid [PersonAge 28]
    p5 <- get pid
    liftIO $ print p5

    updateWhere [PersonNameEq "Michael"] [PersonAge 29]
    p6 <- get pid
    liftIO $ print p6

    _ <- insert $ Person "Eliezer" 2 $ Just "blue"
    p7 <- select [] [PersonAgeAsc]
    liftIO $ print p7

    _ <- insert $ Person "Abe" 30 $ Just "black"
    p8 <- select [PersonAgeLt 30] [PersonNameDesc]
    liftIO $ print p8

    {-
    insertR $ Person "Abe" 31 $ Just "brown"
    p9 <- select [PersonNameEq "Abe"] []
    liftIO $ print p9
    -}

    p10 <- getBy $ PersonNameKey "Michael"
    liftIO $ print p10

    p11 <- select [PersonColorEq $ Just "blue"] []
    liftIO $ print p11

    p12 <- select [PersonColorEq Nothing] []
    liftIO $ print p12

    p13 <- select [PersonColorNe Nothing] []
    liftIO $ print p13

    delete pid
    plast <- get pid
    liftIO $ print plast
