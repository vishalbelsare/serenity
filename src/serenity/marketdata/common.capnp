@0xf3781e15c8fc7585;

enum Side {
    buy @0;
    sell @1;
}

struct Symbol {
    feedCode @0 : Text;
    symbolCode @1 : Text;
}

struct PriceLevel {
    qty @0 : Float64;
    px @1 : Float64;
}

struct ResponsePreamble {
    statusCode @0 : Int64;
    statusMessage @1 : Text;
}