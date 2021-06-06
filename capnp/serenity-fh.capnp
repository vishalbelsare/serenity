@0x8fe87a03768154e9;

enum Side {
    buy @0;
    sell @1;
}

struct TradeMessage {
    time @0 : Float64;
    tradeId @1 : Int64;
    side @2 : Side;
    size @3 : Float64;
    price @4 : Float64;
}

struct Level1BookUpdateMessage {
    time @0 : Float64;
    bestBidQty @1 : Float64;
    bestBidPx @2 : Float64;
    bestAskQty @3 : Float64;
    bestAskPx @4 : Float64;
}
