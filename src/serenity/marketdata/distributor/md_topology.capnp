@0xfee1915d628281ae

using common = import "../common.capnp";

struct DistributorRegistrationRequestMessage {
    serviceId: Text;
    channels: List(Text);
    symbols: List(common.Symbol)
}

struct DistributorRegistrationResponseMessage {
    statusCode: Text;
    statusMessage: Text;
}

struct DistributorTopologyRequestMessage {
    subscriptions: List(Text);
}

struct SubscriptionProviders {
    subscription: Text;
    providerServiceIds: List(Text);
}

struct DistributorTopologyResponseMessage {
    providers: List(SubscriptionProviders);
}