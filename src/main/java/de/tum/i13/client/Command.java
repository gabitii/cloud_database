package de.tum.i13.client;

/**
 * This class is used by Application Handler to define a command that a user
 * has given in CLI. This command are then seen by the client which has
 * different cases depending on the command enum.
 *
 */

public enum Command {
    CONNECT,
    DISCONNECT,
    LOGLEVEL,
    HELP,
    QUIT,
    EMPTY,
    PUT,
    GET,
    KEYRANGE,
    KEYRANGE_READ,
    PUBLISH,
    SUBSCRIBE,
    UNSUBSCRIBE
}