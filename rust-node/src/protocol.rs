// This module exposes all the generated types for the Tron network and LibP2P.
// Due to how tonic-build organizes files by package name, we must match
// the module structure expected by the generated recursive imports.

#[allow(dead_code)]
pub mod protocol {
    // Both core/Tron.proto and api/api.proto use 'package protocol'
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}
#[allow(dead_code)]
pub mod p2p {
    // p2p.rs expects to find types like 'Endpoint' in 'super::protocol'.
    // By re-exporting the 'protocol' module here, we satisfy the dependency.
    include!(concat!(env!("OUT_DIR"), "/p2p.rs"));
}

#[allow(dead_code)]
pub mod api {
    // Current node internal API (package 'api')
    include!(concat!(env!("OUT_DIR"), "/api.rs"));
}

#[allow(dead_code)]
pub mod google {
    pub mod api {
         include!(concat!(env!("OUT_DIR"), "/google.api.rs"));
    }
}
