module Database.Persist.Quasi
    ( parse 
    ) where

import Database.Persist.Base
import Data.Char
import Data.Maybe (mapMaybe)
import Text.ParserCombinators.Parsec hiding (parse, token)
import qualified Text.ParserCombinators.Parsec as Parsec

-- | Parses a quasi-quoted syntax into a list of entity definitions.
parse :: String -> [EntityDef]
parse = map parse' . nest . map words'
      . removeLeadingSpaces
      . map killCarriage
      . lines

removeLeadingSpaces :: [String] -> [String]
removeLeadingSpaces x =
    let y = filter (not . null) x
     in if all isSpace (map head y)
            then removeLeadingSpaces (map tail y)
            else y

killCarriage :: String -> String
killCarriage "" = ""
killCarriage s
    | last s == '\r' = init s
    | otherwise = s

words' :: String -> (Bool, [String])
words' s = case Parsec.parse words'' s s of
            Left e -> error $ show e
            Right x -> x

words'' :: Parser (Bool, [String])
words'' = do
    s <- fmap (not . null) $ many space
    t <- many token
    eof
    return (s, takeWhile (/= "--") t)
  where
    token = do
        t <- (char '"' >> quoted) <|> unquoted
        spaces
        return t
    quoted = do
        s <- many1 $ noneOf "\""
        _ <- char '"'
        return s
    unquoted = many1 $ noneOf " \t"

nest :: [(Bool, [String])] -> [(String, [String], [[String]])]
nest ((False, name:entattribs):rest) =
    let (x, y) = break (not . fst) rest
     in (name, entattribs, map snd x) : nest y
nest ((False, []):_) = error "Indented line must contain at least name"
nest ((True, _):_) = error "Blocks must begin with non-indented lines"
nest [] = []

parse' :: (String, [String], [[String]]) -> EntityDef
parse' (name, entattribs, attribs) =
    EntityDef name entattribs cols uniqs derives
  where
    cols = concatMap takeCols attribs
    uniqs = concatMap takeUniqs attribs
    derives = case mapMaybe takeDerives attribs of
                [] -> ["Show", "Read", "Eq"]
                x -> concat x

takeCols :: [String] -> [(String, String, [String])]
takeCols ("deriving":_) = []
takeCols (n@(f:_):ty:rest)
    | isLower f = [(n, ty, rest)]
takeCols _ = []

takeUniqs :: [String] -> [(String, [String])]
takeUniqs (n@(f:_):rest)
    | isUpper f = [(n, rest)]
takeUniqs _ = []

takeDerives :: [String] -> Maybe [String]
takeDerives ("deriving":rest) = Just rest
takeDerives _ = Nothing
