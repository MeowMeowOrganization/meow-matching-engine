# Contract snapshot

CEX 11 defines `meow.exchange.matching.v1alpha1` as the canonical matching wire package. This directory is a build-local snapshot so the matching-engine repository can compile independently.

The supplied CEX 11 material referenced `meow/exchange/order/v1alpha1/order.proto` but did not include that source file. The local `order.proto` therefore contains only the `OrderSide` dependency needed by the matching messages (`UNSPECIFIED=0`, `BUY=1`, `SELL=2`). Before merging into a multi-repository environment, replace this snapshot with the canonical generated contract dependency (or verify the enum numbers against it) so there is one schema authority.
