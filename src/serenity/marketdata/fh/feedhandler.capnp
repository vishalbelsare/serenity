@0x8fe87a03768154e9;

using Common = import "../common.capnp";

struct TradeMessage {
    time @0 : Float64;
    exchTime @1 : Float64;
    symbol @2 : Common.Symbol;
    tradeId @3 : Int64;
    side @4 : Common.Side;
    size @5 : Float64;
    price @6 : Float64;
}

struct OrderBookSnapshotMessage {
    time @0 : Float64;
    eventId @1 : Int64;
    symbol @2 : Common.Symbol;
    bids @3 : List(Common.PriceLevel);
    asks @4 : List(Common.PriceLevel);
}

struct OrderBookDeltaMessage {
    time @0 : Float64;
    eventId @1: Int64;
    exchTime @2 : Float64;
    symbol @3 : Common.Symbol;
    bidDeltas @4 : List(Common.PriceLevel);
    askDeltas @5 : List(Common.PriceLevel);
}