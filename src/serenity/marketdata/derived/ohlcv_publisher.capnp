@0xca65cdeaa0dc5b8f;

using common = import "../common.capnp";

struct OHLCVMessage {
    time @0 : Float64;
    symbol @2 : common.Symbol;
    openPx @1 : Float64;
    highPx @2 : Float64;
    lowPx @3 : Float64;
    closePx @4 : Float64;
    volume @5 : Float64;
}