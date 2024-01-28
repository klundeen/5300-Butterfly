// Debug utilities for SQL DB manager
// Dominic Burgi
// CPSC 5300 - Butterfly

#ifndef DEBUG_H
#define DEBUG_H

#include <stdio.h>
#include <cstdio>

#ifdef DEBUG_ENABLED
#define DEBUG_OUT(input)                                        \
    do {                                                        \
        printf("[%18s:%-3d] %s", __FILE__, __LINE__, input);    \
    } while (0)
#define DEBUG_OUT_VAR(fmt, ...)                                         \
    do {                                                                \
        printf("[%18s:%-3d] " fmt, __FILE__, __LINE__, __VA_ARGS__);    \
    } while (0)
#else
#define DEBUG_OUT(input)
#define DEBUG_OUT_VAR(fmt, ...)
#endif // DEBUG_ENABLED

#endif // DEBUG_H