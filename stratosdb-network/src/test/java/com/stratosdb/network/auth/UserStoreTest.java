package com.stratosdb.network.auth;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UserStoreTest {

    @Test
    void correctPasswordVerifies() {
        UserStore store = new UserStore();
        store.addUser("alice", "correct-horse-battery-staple");
        assertTrue(store.verify("alice", "correct-horse-battery-staple"));
    }

    @Test
    void wrongPasswordFails() {
        UserStore store = new UserStore();
        store.addUser("alice", "correct-horse-battery-staple");
        assertFalse(store.verify("alice", "wrong-password"));
    }

    @Test
    void unknownUserFails() {
        UserStore store = new UserStore();
        store.addUser("alice", "some-password");
        assertFalse(store.verify("mallory", "anything"));
    }

    @Test
    void twoUsersWithTheSamePasswordGetDifferentStoredHashes() {
        // Not directly observable from the public API (that's the point -
        // there's no getter for the raw hash), but both must independently
        // verify correctly, which would still be true even with a shared
        // salt - so this mainly documents the intent. The real guarantee
        // (independent random salts) is that a rainbow-table attack against
        // one user's hash doesn't help against the other's, which isn't
        // something a single-process unit test can observe directly, but
        // is enforced by addUser() always drawing a fresh SecureRandom salt.
        UserStore store = new UserStore();
        store.addUser("alice", "shared-password");
        store.addUser("bob", "shared-password");
        assertTrue(store.verify("alice", "shared-password"));
        assertTrue(store.verify("bob", "shared-password"));
        assertFalse(store.verify("alice", "wrong"));
        assertFalse(store.verify("bob", "wrong"));
    }

    @Test
    void removedUserNoLongerVerifies() {
        UserStore store = new UserStore();
        store.addUser("alice", "password123");
        assertTrue(store.verify("alice", "password123"));
        store.removeUser("alice");
        assertFalse(store.verify("alice", "password123"));
    }

    @Test
    void hasAnyUsersReflectsStoreState() {
        UserStore store = new UserStore();
        assertFalse(store.hasAnyUsers());
        store.addUser("alice", "x");
        assertTrue(store.hasAnyUsers());
    }
}
