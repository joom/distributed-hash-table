import Test.Hspec
import Test.QuickCheck

import DHT

propIntBytesRead :: Int -> Int -> Bool
propIntBytesRead i j = i' == n
 where
   i' = abs i
   j' = abs j
   max = i' + j'
   s = intWithCompleteBytes i' max
   n = read s :: Int

main :: IO ()
main = hspec $
  describe "Conversion of an Int to a String with max # of bytes" $ do
    it "adds the right number of 0s" $
      intWithCompleteBytes 23 5 `shouldBe` "00023"
    it "results in the same number when it's read from the string" $
      property propIntBytesRead
