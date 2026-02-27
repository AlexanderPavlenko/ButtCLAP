# Buttclap

Same idea as [Buttplug for Live](https://github.com/eira-fransham/Buttplug-for-Live) realized as CLAP – meaning it can run even on Steamdeck in Bitwig.

## Building

After installing [Rust](https://rustup.rs/), you can compile Buttclap as follows:

```shell
cargo xtask bundle buttclap --release
```

## Usage

Requires [Intiface® Central](https://github.com/intiface/intiface-central/releases) v3.
- App Mode: Engine

MIDI NoteOn events trigger "Sample&Hold" of the Level param which can be modulated or remote controlled with any frequency.
Actual update frequency clock can be one note clip playing in a short loop, or any other MIDI generator.

## Troubleshooting

Run DAW with `NIH_LOG` env var to enable logging.
Build without `--release` flag to increase verbosity.

```shell
NIH_LOG=~/nih.log /var/lib/flatpak/app/com.bitwig.BitwigStudio/current/active/files/bitwig-studio
```