//! Which addresses dipper may call an indexer at. An indexer chooses the URL it registers, so
//! unchecked it could point dipper, directly or by a redirect, at a service inside dipper's own
//! network, such as the cloud metadata service. Dipper refuses every address that is not public.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use url::{Host, Url};

/// The addresses dipper may call indexers at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AddressRule {
    /// Public addresses only.
    PublicOnly,
    /// Any address, for a local network whose indexers sit at private addresses.
    Any,
}

impl AddressRule {
    /// The rule that `indexer_urls.allow_private_addresses` sets.
    pub fn new(allow_private_addresses: bool) -> Self {
        if allow_private_addresses {
            Self::Any
        } else {
            Self::PublicOnly
        }
    }

    /// Whether dipper may connect to `ip`.
    pub fn allows(self, ip: IpAddr) -> bool {
        self == Self::Any || is_public(ip)
    }

    /// Refuse `url` if its host is written as an address this rule doesn't allow. A host written
    /// as a name passes here: it is judged by what it resolves to, when dipper connects.
    pub fn check_url(self, url: &Url) -> Result<(), Refused> {
        let ip = match url.host() {
            Some(Host::Ipv4(ip)) => IpAddr::V4(ip),
            Some(Host::Ipv6(ip)) => IpAddr::V6(ip),
            Some(Host::Domain(_)) | None => return Ok(()),
        };
        if self.allows(ip) {
            Ok(())
        } else {
            Err(Refused::Address(ip))
        }
    }
}

/// Dipper's refusal to connect to an address that is not public.
#[derive(Debug, thiserror::Error)]
pub enum Refused {
    /// A host written as an address that is not public.
    #[error("refused {0}: not a public address")]
    Address(IpAddr),
}

/// Whether `ip` is public: reachable across the internet, rather than kept for a private network,
/// the machine itself, documentation or another special purpose.
fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_v4(ip),
        IpAddr::V6(ip) => is_public_v6(ip),
    }
}

/// IPv4 ranges that are not public, from IANA's special-purpose address registry, each as its
/// first address and the number of leading bits every address in it shares with that one.
const NOT_PUBLIC_V4: [(Ipv4Addr, u32); 14] = [
    // "This network".
    (Ipv4Addr::new(0, 0, 0, 0), 8),
    // Private networks.
    (Ipv4Addr::new(10, 0, 0, 0), 8),
    // Shared by an internet provider's customers behind its own address translation.
    (Ipv4Addr::new(100, 64, 0, 0), 10),
    // The machine itself.
    (Ipv4Addr::new(127, 0, 0, 0), 8),
    // A single network link, home to the cloud metadata service at 169.254.169.254.
    (Ipv4Addr::new(169, 254, 0, 0), 16),
    // Private networks.
    (Ipv4Addr::new(172, 16, 0, 0), 12),
    // Protocol assignments.
    (Ipv4Addr::new(192, 0, 0, 0), 24),
    // Documentation.
    (Ipv4Addr::new(192, 0, 2, 0), 24),
    // Private networks.
    (Ipv4Addr::new(192, 168, 0, 0), 16),
    // Benchmarking.
    (Ipv4Addr::new(198, 18, 0, 0), 15),
    // Documentation.
    (Ipv4Addr::new(198, 51, 100, 0), 24),
    (Ipv4Addr::new(203, 0, 113, 0), 24),
    // Multicast.
    (Ipv4Addr::new(224, 0, 0, 0), 4),
    // Reserved, and the broadcast address.
    (Ipv4Addr::new(240, 0, 0, 0), 4),
];

/// The IPv6 range that holds every public address. The rest of IPv6 is unassigned, or kept for a
/// single network or machine, such as `::1`, `fe80::/10` and `fc00::/7`.
const GLOBAL_UNICAST_V6: (Ipv6Addr, u32) = (Ipv6Addr::new(0x2000, 0, 0, 0, 0, 0, 0, 0), 3);

/// Ranges inside the public IPv6 range that are not public either.
const NOT_PUBLIC_V6: [(Ipv6Addr, u32); 3] = [
    // Protocol assignments, such as tunnels and benchmarking.
    (Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 23),
    // Documentation.
    (Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 0), 32),
    (Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20),
];

/// `::ffff:a.b.c.d`, which a socket connects to over IPv4, at `a.b.c.d`.
const MAPPED_V4: (Ipv6Addr, u32) = (Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0, 0), 96);

/// `64:ff9b::a.b.c.d`, which an IPv6 to IPv4 gateway forwards to `a.b.c.d`.
const NAT64: (Ipv6Addr, u32) = (Ipv6Addr::new(0x64, 0xff9b, 0, 0, 0, 0, 0, 0), 96);

/// `2002:aabb:ccdd::/48`, which is tunnelled over IPv4 to `aa.bb.cc.dd`.
const SIX_TO_FOUR: (Ipv6Addr, u32) = (Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16);

fn is_public_v4(ip: Ipv4Addr) -> bool {
    !NOT_PUBLIC_V4.iter().any(|&range| in_v4(ip, range))
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    // An address that carries an IPv4 address inside, and is delivered to it, is as public as that
    // IPv4 address.
    let [.., a, b, c, d] = ip.octets();
    if in_v6(ip, MAPPED_V4) || in_v6(ip, NAT64) {
        return is_public_v4(Ipv4Addr::new(a, b, c, d));
    }
    if in_v6(ip, SIX_TO_FOUR) {
        let [_, _, a, b, c, d, ..] = ip.octets();
        return is_public_v4(Ipv4Addr::new(a, b, c, d));
    }
    in_v6(ip, GLOBAL_UNICAST_V6) && !NOT_PUBLIC_V6.iter().any(|&range| in_v6(ip, range))
}

/// Whether `ip` shares its first `bits` bits with `first`.
fn in_v4(ip: Ipv4Addr, (first, bits): (Ipv4Addr, u32)) -> bool {
    let mask = u32::MAX.checked_shl(32 - bits).unwrap_or(0);
    u32::from(ip) & mask == u32::from(first) & mask
}

/// Whether `ip` shares its first `bits` bits with `first`.
fn in_v6(ip: Ipv6Addr, (first, bits): (Ipv6Addr, u32)) -> bool {
    let mask = u128::MAX.checked_shl(128 - bits).unwrap_or(0);
    u128::from(ip) & mask == u128::from(first) & mask
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_public_addresses_are_allowed_by_default() {
        let not_public = [
            "0.0.0.0",
            "0.255.255.255",
            "10.0.0.5",
            "10.255.255.255",
            "100.64.0.0",
            // Alibaba Cloud's metadata service.
            "100.100.100.200",
            "100.127.255.255",
            "127.0.0.1",
            "127.255.255.255",
            // The metadata service of AWS, Google Cloud and Azure.
            "169.254.169.254",
            "172.16.0.0",
            "172.31.255.255",
            "192.0.0.1",
            "192.0.2.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "239.255.255.255",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            // `::a.b.c.d`, an IPv6 address carrying an IPv4 one without the `ffff` marker.
            "::a00:5",
            "64:ff9b::a00:5",
            "64:ff9b:1::808:808",
            "100::1",
            "2001::1",
            "2001:1ff:ffff::1",
            "2001:db8::1",
            "2002:a00:5::1",
            "2002:7f00:1::1",
            "3fff::1",
            "fc00::1",
            // AWS's metadata service over IPv6.
            "fd00:ec2::254",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "1fff:ffff::1",
            "4000::1",
        ];
        let public = [
            "1.1.1.1",
            "8.8.8.8",
            "9.255.255.255",
            "11.0.0.0",
            "100.63.255.255",
            "100.128.0.0",
            "126.255.255.255",
            "128.0.0.0",
            "169.253.255.255",
            "169.255.0.0",
            "172.15.255.255",
            "172.32.0.0",
            "192.0.1.0",
            "192.0.3.0",
            "192.167.255.255",
            "192.169.0.0",
            "198.17.255.255",
            "198.20.0.0",
            "198.51.99.255",
            "198.51.101.0",
            "203.0.112.255",
            "203.0.114.0",
            "223.255.255.255",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2001:200::1",
            "2001:4860:4860::8888",
            "2002:808:808::1",
            "2606:4700:4700::1111",
            "2a00:1450:4009::1",
            "3ffe:ffff::1",
        ];

        for (addresses, expected) in [(&not_public[..], false), (&public[..], true)] {
            for address in addresses {
                let ip: IpAddr = address.parse().unwrap();
                assert_eq!(is_public(ip), expected, "{address}");
                assert_eq!(AddressRule::PublicOnly.allows(ip), expected, "{address}");
                assert!(AddressRule::Any.allows(ip), "{address}");
            }
        }
    }

    #[test]
    fn a_url_is_judged_by_the_address_written_in_it_and_a_name_is_left_for_connecting() {
        for (url, refused) in [
            ("http://10.0.0.5:8545/", Some("10.0.0.5")),
            (
                "http://169.254.169.254/latest/meta-data/",
                Some("169.254.169.254"),
            ),
            ("http://[::1]:7600/", Some("::1")),
            ("http://[::ffff:7f00:1]/", Some("::ffff:127.0.0.1")),
            // 127.0.0.1 written as 1 number, and in hexadecimal.
            ("http://2130706433/", Some("127.0.0.1")),
            ("http://0x7f.1/", Some("127.0.0.1")),
            ("https://8.8.8.8/", None),
            ("https://indexer.example.com/", None),
            ("http://localhost/", None),
        ] {
            let url: Url = url.parse().unwrap();
            let refusal = AddressRule::PublicOnly
                .check_url(&url)
                .err()
                .map(|refused| refused.to_string());
            let expected = refused.map(|ip| format!("refused {ip}: not a public address"));
            assert_eq!(refusal, expected, "{url}");
            assert!(AddressRule::Any.check_url(&url).is_ok(), "{url}");
        }
    }

    #[test]
    fn the_switch_sets_the_rule() {
        assert_eq!(AddressRule::new(false), AddressRule::PublicOnly);
        assert_eq!(AddressRule::new(true), AddressRule::Any);
    }
}
