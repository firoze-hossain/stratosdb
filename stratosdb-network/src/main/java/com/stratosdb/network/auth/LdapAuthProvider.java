package com.stratosdb.network.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import java.util.Hashtable;

/**
 * External LDAP / Active Directory authentication provider using JNDI LDAPv3.
 * Supports direct DN/UPN bind patterns (OpenLDAP {@code uid={0},ou=users,dc=example,dc=com}
 * or Active Directory {@code {0}@corp.example.com}) as well as service-account search-then-bind.
 */
public final class LdapAuthProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LdapAuthProvider.class);

    private final LdapConfig config;

    public LdapAuthProvider(LdapConfig config) {
        this.config = config;
    }

    public boolean authenticate(String username, String password) {
        if (username == null || username.isBlank() || password == null || password.isEmpty()) {
            // Reject empty credentials to prevent anonymous LDAP bind bypass
            return false;
        }
        try {
            String bindPrincipal = resolveBindPrincipal(username);
            if (bindPrincipal == null) {
                return false;
            }
            DirContext ctx = createContext(bindPrincipal, password);
            ctx.close();
            return true;
        } catch (NamingException e) {
            LOG.debug("LDAP authentication failed for user {}: {}", username, e.getMessage());
            return false;
        }
    }

    private String resolveBindPrincipal(String username) throws NamingException {
        if (config.getSearchBase() != null && !config.getSearchBase().isBlank()
                && config.getSearchFilter() != null && !config.getSearchFilter().isBlank()) {
            DirContext serviceCtx = createContext(config.getServiceBindDn(), config.getServiceBindPassword());
            try {
                SearchControls controls = new SearchControls();
                controls.setSearchScope(SearchControls.SUBTREE_SCOPE);
                controls.setCountLimit(1);
                String filter = config.getSearchFilter().replace("{0}", escapeLdapFilter(username));
                NamingEnumeration<SearchResult> results = serviceCtx.search(config.getSearchBase(), filter, controls);
                if (!results.hasMore()) {
                    return null;
                }
                return results.next().getNameInNamespace();
            } finally {
                serviceCtx.close();
            }
        }
        return config.getBindDnPattern().replace("{0}", escapeLdapDnValue(username));
    }

    private DirContext createContext(String principal, String credentials) throws NamingException {
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, config.getLdapUrl());
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        if (principal != null) {
            env.put(Context.SECURITY_PRINCIPAL, principal);
        }
        if (credentials != null) {
            env.put(Context.SECURITY_CREDENTIALS, credentials);
        }
        env.put("com.sun.jndi.ldap.connect.timeout", String.valueOf(config.getConnectTimeoutMs()));
        env.put("com.sun.jndi.ldap.read.timeout", String.valueOf(config.getReadTimeoutMs()));
        return new InitialDirContext(env);
    }

    private static String escapeLdapDnValue(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\', ',', '+', '"', '<', '>', ';', '#' -> sb.append('\\').append(c);
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String escapeLdapFilter(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\5c");
                case '*' -> sb.append("\\2a");
                case '(' -> sb.append("\\28");
                case ')' -> sb.append("\\29");
                case '\0' -> sb.append("\\00");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }
}
