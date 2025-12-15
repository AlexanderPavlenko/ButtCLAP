use buttplug_client::{
    connector::ButtplugRemoteClientConnector, ButtplugClient, ButtplugClientDevice,
    ButtplugClientEvent,
};
use buttplug_transport_websocket_tungstenite::ButtplugWebsocketClientTransport;
use crossbeam::channel::{Receiver, Sender};
use evmap;
use futures::StreamExt;
use nih_plug::prelude::*;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio;
use tokio::task;
use tokio::task::yield_now;
use tokio::time::sleep;

static START: Once = Once::new();

struct Buttclap {
    params: Arc<ButtclapParams>,
    intiface_url: String,
    // intiface_devices: (
    //     evmap::handles::WriteHandle<String, Device>,
    //     evmap::handles::ReadHandle<String, Device>,
    // ),
    channel: (Sender<f32>, Receiver<f32>),
}

// https://github.com/robbert-vdh/nih-plug/pull/106/files
enum ButtclapBackgroundTask {
    IntifaceConnection,
}

#[derive(Params)]
struct ButtclapParams {
    /// The parameter's ID is used to identify the parameter in the wrapped plugin API. As long as
    /// these IDs remain constant, you can rename and reorder these fields as you wish. The
    /// parameters are exposed to the host in the same order they were defined.
    #[id = "level"]
    pub level: FloatParam,
}

impl Default for Buttclap {
    fn default() -> Self {
        Self {
            params: Arc::new(ButtclapParams::default()),
            intiface_url: String::from("ws://127.0.0.1:12345"),
            channel: crossbeam::channel::bounded(1),
        }
    }
}

impl Default for ButtclapParams {
    fn default() -> Self {
        Self {
            level: FloatParam::new("Level", 0.0, FloatRange::Linear { min: 0.0, max: 1.0 }),
        }
    }
}

impl Plugin for Buttclap {
    const NAME: &'static str = "Buttclap";
    const VENDOR: &'static str = "lexi.flvr.top";
    const URL: &'static str = env!("CARGO_PKG_HOMEPAGE");
    const EMAIL: &'static str = "x.ywtop@slmail.me";

    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    const AUDIO_IO_LAYOUTS: &'static [AudioIOLayout] = &[AudioIOLayout::const_default()];
    const MIDI_INPUT: MidiConfig = MidiConfig::Basic;
    const MIDI_OUTPUT: MidiConfig = MidiConfig::None;

    const SAMPLE_ACCURATE_AUTOMATION: bool = true;
    const HARD_REALTIME_ONLY: bool = true;

    // If the plugin can send or receive SysEx messages, it can define a type to wrap around those
    // messages here. The type implements the `SysExMessage` trait, which allows conversion to and
    // from plain byte buffers.
    type SysExMessage = ();
    // More advanced plugins can use this to run expensive background tasks. See the field's
    // documentation for more information. `()` means that the plugin does not have any background
    // tasks.
    type BackgroundTask = ButtclapBackgroundTask;

    // Implement the plugin's task runner by switching on task name.
    //   - Called after the plugin instance is created
    //   - Send result back over a channel or triple buffer
    fn task_executor(&mut self) -> TaskExecutor<Self> {
        nih_dbg!("task_executor");
        let intiface_url = self.intiface_url.clone();
        let channel = self.channel.1.clone();
        Box::new(move |task| match task {
            ButtclapBackgroundTask::IntifaceConnection => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                    .unwrap();
                let intiface_url = intiface_url.clone();
                let channel = channel.clone();

                runtime.block_on(async move {
                    nih_dbg!("runtime.block_on");
                    let intiface_url = intiface_url.clone();
                    let channel = channel.clone();
                    let local = task::LocalSet::new();
                    let local_task = async move {
                        let (mut devices_mut, devices) =
                            unsafe { evmap::new_assert_stable::<String, Device>() };
                        let intiface_url = intiface_url.clone();
                        let channel = channel.clone();

                        let intiface_task = async move {
                            loop {
                                nih_dbg!("intiface_task loop");
                                let client = ButtplugClient::new("buttclap");
                                let connector = ButtplugRemoteClientConnector::<
                                    ButtplugWebsocketClientTransport,
                                >::new(
                                    ButtplugWebsocketClientTransport::new_insecure_connector(
                                        &intiface_url,
                                    ),
                                );

                                match client.connect(connector).await {
                                    Ok(_) => {
                                        match client.start_scanning().await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                nih_dbg!(e);
                                                sleep(Duration::from_secs(1)).await;
                                                continue; // re-connect in a loop
                                            }
                                        }

                                        let event_stream = client.event_stream();
                                        futures::pin_mut!(event_stream);
                                        let intiface_event_loop = async {
                                            nih_dbg!("intiface_event_loop");
                                            while let Some(event) = event_stream.next().await {
                                                nih_dbg!(&event);
                                                match event {
                                                    ButtplugClientEvent::DeviceAdded(device) => {
                                                        // let name = Box::leak(
                                                        //     normalize_device_name(device.name())
                                                        //         .into_boxed_str(),
                                                        // );
                                                        let name = crate::normalize_device_name(
                                                            device.name(),
                                                        );
                                                        devices_mut.update(
                                                            name,
                                                            Device {
                                                                device: Arc::new(device),
                                                            },
                                                        );
                                                        // devices.update(
                                                        //     DEVICES_LAST,
                                                        //     Device {
                                                        //         device: device.clone(),
                                                        //     },
                                                        // );
                                                        devices_mut.publish();
                                                        // info!("[{}] added", name);
                                                        yield_now().await;
                                                    }
                                                    ButtplugClientEvent::DeviceRemoved(_device) => {
                                                        // warn!("[{}] removed", normalize_device_name(&device.name));
                                                        // rescanning, maybe a temporary disconnect
                                                        let _ = client.stop_scanning().await;
                                                        let _ = client.start_scanning().await;
                                                    }
                                                    ButtplugClientEvent::ServerDisconnect
                                                    | ButtplugClientEvent::Error(_) => {
                                                        devices_mut.purge();
                                                        devices_mut.publish();
                                                        sleep(Duration::from_secs(1)).await;
                                                        continue; // re-connect in a loop
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        };

                                        intiface_event_loop.await;
                                    }
                                    Err(e) => {
                                        nih_dbg!(e);
                                        sleep(Duration::from_secs(1)).await;
                                        continue; // re-connect in a loop
                                    }
                                }
                            }
                        };

                        let modulation_task = async move {
                            nih_dbg!("modulation_task");
                            loop {
                                match channel.recv_timeout(Duration::from_millis(100)) {
                                    Ok(level) => {
                                        nih_dbg!(level);
                                        match devices.enter() {
                                            Some(devices) => {
                                                for (_name, value) in devices.iter() {
                                                    match value.get_one() {
                                                        Some(device) => {
                                                            nih_dbg!(device);
                                                            match device.vibrate(level as f64).await {
                                                                Ok(_) => {}
                                                                Err(e) => { nih_dbg!(e); }
                                                            };
                                                        }
                                                        _ => {}
                                                    };
                                                }
                                            }
                                            None => {}
                                        };
                                    }
                                    Err(_) => {}
                                }
                                yield_now().await;
                            }
                        };

                        task::spawn_local(intiface_task);
                        task::spawn_local(modulation_task);
                    };

                    local.run_until(local_task).await;
                    local.await;
                });
                nih_dbg!("Unexpected: runtime.block_on should not return");
            }
        })
    }

    fn params(&self) -> Arc<dyn Params> {
        self.params.clone()
    }

    // fn initialize(
    //     &mut self,
    //     _audio_io_layout: &AudioIOLayout,
    //     _buffer_config: &BufferConfig,
    //     _context: &mut impl InitContext<Self>,
    // ) -> bool {
    //     // Resize buffers and perform other potentially expensive initialization operations here.
    //     // The `reset()` function is always called right after this function. You can remove this
    //     // function if you do not need it.
    //     true
    // }

    // fn reset(&mut self) {
    //     // Reset buffers and envelopes here. This can be called from the audio thread and may not
    //     // allocate. You can remove this function if you do not need it.
    // }

    fn process(
        &mut self,
        _buffer: &mut Buffer,
        _aux: &mut AuxiliaryBuffers,
        context: &mut impl ProcessContext<Self>,
    ) -> ProcessStatus {
        START.call_once(|| {
            nih_dbg!("START.call_once");
            context.execute_background(Self::BackgroundTask::IntifaceConnection);
        });
        while let Some(NoteEvent::NoteOn { .. }) = context.next_event() {
            match self.channel.0.try_send(self.params.level.value()) {
                Ok(_) => {}
                Err(e) => {
                    nih_dbg!(e);
                }
            }
        }
        ProcessStatus::KeepAlive
    }
}

impl ClapPlugin for Buttclap {
    const CLAP_ID: &'static str = "top.flvr.buttclap";
    const CLAP_DESCRIPTION: Option<&'static str> = Some("Control things via Intiface® Central");
    const CLAP_MANUAL_URL: Option<&'static str> = Some(Self::URL);
    const CLAP_SUPPORT_URL: Option<&'static str> = None;
    const CLAP_FEATURES: &'static [ClapFeature] = &[ClapFeature::NoteEffect, ClapFeature::Utility];

    fn remote_controls(&self, context: &mut impl RemoteControlsContext) {
        context.add_section("Section", |section| {
            section.add_page("Page", |page| {
                page.add_param(&self.params.level);
            })
        })
    }
}

// impl Vst3Plugin for Buttclap {
//     const VST3_CLASS_ID: [u8; 16] = *b"top.flvr.buttclp";
//
//     // And also don't forget to change these categories
//     const VST3_SUBCATEGORIES: &'static [Vst3SubCategory] =
//         &[Vst3SubCategory::Fx, Vst3SubCategory::Tools];
// }

nih_export_clap!(Buttclap);
// nih_export_vst3!(Buttclap);

fn normalize_device_name(name: &str) -> String {
    name.split(|c: char| !c.is_alphanumeric())
        .collect::<String>()
}

#[derive(Debug, Eq, Clone)]
struct Device {
    device: Arc<ButtplugClientDevice>,
}

impl std::hash::Hash for Device {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.device.name().hash(state);
    }
}

impl PartialEq for Device {
    fn eq(&self, other: &Self) -> bool {
        self.device.eq(&other.device)
    }
}

impl Deref for Device {
    type Target = Arc<ButtplugClientDevice>;

    fn deref(&self) -> &Self::Target {
        &self.device
    }
}
