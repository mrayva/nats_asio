# Shim for Arrow's own Findutf8proc.cmake, which - under vcpkg's toolchain
# (VCPKG_TOOLCHAIN is set project-wide here) - specifically looks for a
# package named "unofficial-utf8proc" providing a plain `utf8proc` target
# to alias as `utf8proc::utf8proc` (see
# /usr/lib/x86_64-linux-gnu/cmake/Arrow/Findutf8proc.cmake).
#
# The vcpkg utf8proc port (2.11.3) already ships its own modern
# `utf8proc::utf8proc` target directly, which collides with Arrow's own
# ALIAS creation for the same name ("_add_library cannot create ALIAS
# target ... because another target with the same name already exists" -
# confirmed via an actual failed configure, not guessed). Rather than
# fighting that version/naming mismatch, this points Arrow's shim at the
# system libutf8proc-dev package (already installed - this project uses
# system Arrow too, not a vcpkg-built one, for the same reason: too large
# to build from source here) via a minimal IMPORTED target under the exact
# name ("utf8proc", no namespace) Arrow's shim expects to alias.
find_path(UTF8PROC_INCLUDE_DIR NAMES utf8proc.h)
find_library(UTF8PROC_LIBRARY NAMES utf8proc)

if(UTF8PROC_INCLUDE_DIR AND UTF8PROC_LIBRARY AND NOT TARGET utf8proc)
    add_library(utf8proc UNKNOWN IMPORTED)
    set_target_properties(utf8proc PROPERTIES
        IMPORTED_LOCATION "${UTF8PROC_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${UTF8PROC_INCLUDE_DIR}"
    )
    set(utf8proc_FOUND TRUE)
else()
    set(utf8proc_FOUND FALSE)
endif()
