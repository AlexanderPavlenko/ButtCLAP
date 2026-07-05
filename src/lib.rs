use buttplug_client::device::{ClientDeviceCommandValue, ClientDeviceOutputCommand};
use buttplug_client::{
    connector::ButtplugRemoteClientConnector, ButtplugClient, ButtplugClientDevice,
    ButtplugClientEvent,
};
use buttplug_transport_websocket_tungstenite::ButtplugWebsocketClientTransport;
use crossbeam::channel::{bounded, Receiver, Sender};
use evmap;
use evmap::handles::{ReadHandle, WriteHandle};
use futures::{Stream, StreamExt};
use nice_plug::prelude::*;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio;
use tokio::task;
use tokio::task::yield_now;

static START: Once = Once::new();

struct Buttclap {
    params: Arc<ButtclapParams>,
    intiface_url: String,
    channel: (Sender<f32>, Receiver<f32>),
}

enum ButtclapBackgroundTask {
    Process,
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
            channel: bounded(1),
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
        nice_dbg!("task_executor");
        let intiface_url = self.intiface_url.clone();
        let channel = self.channel.1.clone();
        Box::new(move |task| match task {
            ButtclapBackgroundTask::Process => {
                background_process(intiface_url.clone(), channel.clone());
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
            nice_dbg!("process START.call_once");
            context.execute_background(Self::BackgroundTask::Process);
        });
        while let Some(NoteEvent::NoteOn { .. }) = context.next_event() {
            match self.channel.0.try_send(self.params.level.value()) {
                Ok(..) => {}
                Err(err) => {
                    nice_dbg!(err);
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

nice_export_clap!(Buttclap);
// nice_export_vst3!(Buttclap);

fn background_process(intiface_url: String, channel: Receiver<f32>) {
    nice_dbg!("background_process");
    tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .unwrap()
        .block_on(async move {
            nice_dbg!("background_process runtime.block_on");
            let local = task::LocalSet::new();
            local
                .run_until(async move {
                    let (devices_w, devices) = unsafe { evmap::new_assert_stable::<u32, Device>() };
                    task::spawn_local(intiface_task(intiface_url, devices_w));
                    task::spawn_local(modulation_task(channel, devices));
                })
                .await;
            local.await;
        });
    nice_dbg!("Unexpected: background_process should not return");
}

async fn intiface_task(intiface_url: String, mut devices: WriteHandle<u32, Device>) {
    loop {
        nice_dbg!("intiface_task loop");
        let client = ButtplugClient::new("buttclap");
        let connector = ButtplugRemoteClientConnector::<ButtplugWebsocketClientTransport>::new(
            ButtplugWebsocketClientTransport::new_insecure_connector(&intiface_url),
        );
        // subscribing before connecting to prevent "[ERROR] Client event DeviceAdded(ButtplugClientDevice {..}) dropped, no client event listener available"
        let event_stream = client.event_stream();

        match client.connect(connector).await {
            Ok(..) => {
                intiface_event_loop(client, event_stream, &mut devices).await;
            }
            Err(err) => {
                nice_dbg!(err);
            }
        }

        devices.purge();
        devices.publish();
        wait_a_sec().await;
    }
}

async fn intiface_event_loop<S: Stream<Item = ButtplugClientEvent>>(
    client: ButtplugClient,
    event_stream: S,
    devices: &mut WriteHandle<u32, Device>,
) where
    <S as Stream>::Item: std::fmt::Debug,
{
    nice_dbg!("intiface_event_loop");
    futures::pin_mut!(event_stream);

    match client.start_scanning().await {
        Ok(..) => {}
        Err(err) => {
            nice_dbg!(err);
        }
    }

    while let Some(event) = event_stream.next().await {
        nice_dbg!(&event);
        match event {
            ButtplugClientEvent::DeviceAdded(device) => {
                devices.update(
                    device.index(),
                    Device {
                        device: Arc::new(device),
                    },
                );
                devices.publish();
                yield_now().await;
            }
            ButtplugClientEvent::DeviceRemoved(_device) => {
                // rescanning, maybe a temporary disconnect
                let _ = client.stop_scanning().await;
                let _ = client.start_scanning().await;
            }
            ButtplugClientEvent::ServerDisconnect => {
                return; // reconnect in a loop
            }
            ButtplugClientEvent::Error(err) => {
                nice_dbg!(err);
                return; // reconnect in a loop
            }
            _ => {}
        }
    }
}

async fn modulation_task(channel: Receiver<f32>, devices: ReadHandle<u32, Device>) {
    nice_dbg!("modulation_task");
    loop {
        if let Ok(level) = channel.recv_timeout(Duration::from_millis(10)) {
            nice_dbg!(level);
            if let Some(devices) = devices.enter() {
                for (_name, value) in devices.iter() {
                    if let Some(device) = value.get_one() {
                        nice_dbg!(device);
                        let result = device
                            .run_output(&ClientDeviceOutputCommand::Vibrate(
                                ClientDeviceCommandValue::Percent(level as f64),
                            ))
                            .await;
                        match result {
                            Ok(..) => {}
                            Err(err) => {
                                nice_dbg!(err);
                            }
                        };
                    };
                }
            };
        }
        yield_now().await;
    }
}

async fn wait_a_sec() {
    tokio::time::sleep(Duration::from_secs(1)).await
}

// fn normalize_device_name(name: &str) -> String {
//     name.split(|c: char| !c.is_alphanumeric())
//         .collect::<String>()
// }

#[derive(Debug, Eq, Clone)]
struct Device {
    device: Arc<ButtplugClientDevice>,
}

impl std::hash::Hash for Device {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.device.index().hash(state);
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
