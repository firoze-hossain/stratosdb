package com.stratosdb.storage.page;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Native INET / CIDR network address representation supporting IPv4 (4 bytes)
 * and IPv6 (16 bytes) with prefix length and PostgreSQL subnet containment
 * operators ({@code <<}, {@code <<=}, {@code >>}, {@code >>=}).
 */
public final class NetworkAddress {
    private final byte[] addressBytes;
    private final int prefixLength;
    private final boolean cidr;
    private final boolean hasExplicitPrefix;

    public NetworkAddress(byte[] addressBytes, int prefixLength, boolean cidr, boolean hasExplicitPrefix) {
        this.addressBytes = Arrays.copyOf(addressBytes, addressBytes.length);
        this.prefixLength = prefixLength;
        this.cidr = cidr;
        this.hasExplicitPrefix = hasExplicitPrefix;
    }

    public static NetworkAddress parse(String raw, boolean isCidr) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException("Empty " + (isCidr ? "CIDR" : "INET") + " value");
        }
        String trimmed = raw.trim();
        String addressPart = trimmed;
        Integer prefix = null;
        boolean explicitPrefix = false;
        int slashIdx = trimmed.indexOf('/');
        if (slashIdx >= 0) {
            explicitPrefix = true;
            addressPart = trimmed.substring(0, slashIdx);
            try {
                prefix = Integer.parseInt(trimmed.substring(slashIdx + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid prefix length in network address: \"" + raw + "\"");
            }
        } else if (isCidr) {
            throw new IllegalArgumentException("Invalid CIDR: \"" + raw + "\" is missing its required /prefix-length");
        }

        InetAddress inet;
        try {
            inet = InetAddress.getByName(addressPart);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Invalid IP address: \"" + addressPart + "\"");
        }
        int maxPrefix = (inet instanceof Inet6Address) ? 128 : 32;
        int finalPrefix = (prefix != null) ? prefix : maxPrefix;
        if (finalPrefix < 0 || finalPrefix > maxPrefix) {
            throw new IllegalArgumentException("Prefix length " + finalPrefix + " out of range (0-" + maxPrefix + ")");
        }
        return new NetworkAddress(inet.getAddress(), finalPrefix, isCidr, explicitPrefix);
    }

    public byte[] getAddressBytes() {
        return Arrays.copyOf(addressBytes, addressBytes.length);
    }

    public int getPrefixLength() {
        return prefixLength;
    }

    public boolean isCidr() {
        return cidr;
    }

    public boolean hasExplicitPrefix() {
        return hasExplicitPrefix;
    }

    /**
     * Strict subnet containment ({@code <<}): true if {@code this} is strictly
     * contained within {@code other} (same address family, {@code this.prefixLength > other.prefixLength},
     * and all {@code other.prefixLength} network bits match).
     */
    public boolean isContainedBy(NetworkAddress other) {
        return other != null
            && this.addressBytes.length == other.addressBytes.length
            && this.prefixLength > other.prefixLength
            && matchesPrefix(this.addressBytes, other.addressBytes, other.prefixLength);
    }

    /**
     * Subnet containment or equality ({@code <<=}): true if {@code this} is
     * contained within or equal to {@code other}.
     */
    public boolean isContainedByOrEquals(NetworkAddress other) {
        return other != null
            && this.addressBytes.length == other.addressBytes.length
            && this.prefixLength >= other.prefixLength
            && matchesPrefix(this.addressBytes, other.addressBytes, other.prefixLength);
    }

    /** Strict supernet ({@code >>}). */
    public boolean contains(NetworkAddress other) {
        return other != null && other.isContainedBy(this);
    }

    /** Supernet or equality ({@code >>=}). */
    public boolean containsOrEquals(NetworkAddress other) {
        return other != null && other.isContainedByOrEquals(this);
    }

    private static boolean matchesPrefix(byte[] a, byte[] b, int prefixBits) {
        int fullBytes = prefixBits / 8;
        for (int i = 0; i < fullBytes; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        int remBits = prefixBits % 8;
        if (remBits > 0) {
            int mask = 0xFF << (8 - remBits);
            return (a[fullBytes] & mask) == (b[fullBytes] & mask);
        }
        return true;
    }

    @Override
    public String toString() {
        try {
            String host = InetAddress.getByAddress(addressBytes).getHostAddress();
            return (cidr || hasExplicitPrefix) ? host + "/" + prefixLength : host;
        } catch (UnknownHostException e) {
            return Arrays.toString(addressBytes) + "/" + prefixLength;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof NetworkAddress other)) return false;
        return this.prefixLength == other.prefixLength
            && Arrays.equals(this.addressBytes, other.addressBytes);
    }

    @Override
    public int hashCode() {
        return 31 * Arrays.hashCode(addressBytes) + prefixLength;
    }
}
