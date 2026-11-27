package com.stratosdb.network.auth;

/**
 * Configuration for external LDAP / Active Directory authentication.
 */
public final class LdapConfig {
    private final String ldapUrl;
    private final String bindDnPattern;
    private final String searchBase;
    private final String searchFilter;
    private final String serviceBindDn;
    private final String serviceBindPassword;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    public LdapConfig(String ldapUrl, String bindDnPattern) {
        this(ldapUrl, bindDnPattern, null, null, null, null, 3000, 3000);
    }

    public LdapConfig(
            String ldapUrl,
            String bindDnPattern,
            String searchBase,
            String searchFilter,
            String serviceBindDn,
            String serviceBindPassword,
            int connectTimeoutMs,
            int readTimeoutMs) {
        this.ldapUrl = ldapUrl;
        this.bindDnPattern = bindDnPattern;
        this.searchBase = searchBase;
        this.searchFilter = searchFilter;
        this.serviceBindDn = serviceBindDn;
        this.serviceBindPassword = serviceBindPassword;
        this.connectTimeoutMs = connectTimeoutMs > 0 ? connectTimeoutMs : 3000;
        this.readTimeoutMs = readTimeoutMs > 0 ? readTimeoutMs : 3000;
    }

    public String getLdapUrl() { return ldapUrl; }
    public String getBindDnPattern() { return bindDnPattern; }
    public String getSearchBase() { return searchBase; }
    public String getSearchFilter() { return searchFilter; }
    public String getServiceBindDn() { return serviceBindDn; }
    public String getServiceBindPassword() { return serviceBindPassword; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public int getReadTimeoutMs() { return readTimeoutMs; }
}
