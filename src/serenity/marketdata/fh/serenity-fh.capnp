@0x8fe87a03768154e9;

enum Side {
    buy @0;
    sell @1;
}

struct RawFeedTradeMessage {
    recvTime @0 : Float64;
    exchTime @1 : Float64;
    feedCode @2 : Text;
    symbolCode @3 : Text;
    tradeId @4 : Int64;
    side @5 : Side;
    size @6 : Float64;
    price @7 : Float64;
}

struct TradeMessage {
    time @0 : Float64;
    tradeId @1 : Int64;
    side @2 : Side;
    size @3 : Float64;
    price @4 : Float64;
}

struct PriceLevel {
    qty @0 : Float64;
    px @1 : Float64;
}

struct RawFeedBookDeltaMessage {
    recvTime @0 : Float64;
    exchTime @1 : Float64;
    feedCode @2 : Text;
    symbolCode @3 : Text;
    bidDeltas @4 : List(PriceLevel);
    askDeltas @5 : List(PriceLevel);
}

struct Level1BookUpdateMessage {
    time @0 : Float64;
    bestBidQty @1 : Float64;
    bestBidPx @2 : Float64;
    bestAskQty @3 : Float64;
    bestAskPx @4 : Float64;
}
