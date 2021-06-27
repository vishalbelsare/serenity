@0xca65cdeaa0dc5b8f;

using common = import "../common.capnp";

struct TickerMessage {
    time @0 : Float64;
    symbol @2 : common.Symbol;
    bestBidQty @1 : Float64;
    bestBidPx @2 : Float64;
    bestAskQty @3 : Float64;
    bestAskPx @4 : Float64;
}