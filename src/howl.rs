use crate::KafkaOption;
use std::thread;
use std::time::{Duration, SystemTime};

use flatbuffers::FlatBufferBuilder;
use isis_streaming_data_types::flatbuffers_generated::events_ev44::{
    Event44Message, Event44MessageArgs, finish_event_44_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::pulse_metadata_pu00::{
    Pu00Message, Pu00MessageArgs, finish_pu_00_message_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::{
    RunStart, RunStartArgs, SpectraDetectorMapping, SpectraDetectorMappingArgs,
    finish_run_start_buffer,
};
use isis_streaming_data_types::flatbuffers_generated::run_stop_6s4t::{
    RunStop, RunStopArgs, finish_run_stop_buffer,
};
use log::{debug, error, info, warn};
use rand::RngExt;
use rand::prelude::ThreadRng;
use rand_distr::{Distribution, Normal};
use rdkafka::ClientConfig;
use rdkafka::producer::{BaseRecord, DefaultProducerContext, ThreadedProducer};
use serde_json::json;
use uuid::Uuid;

fn generate_run_start<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    det_max: i32,
    event_topic: &str,
    job_id: &str,
    stop_time: Option<u64>,
) -> &'a [u8] {
    fbb.reset();
    let args = SpectraDetectorMappingArgs {
        spectrum: Some(fbb.create_vector(&(0..=det_max).collect::<Vec<_>>())),
        detector_id: Some(fbb.create_vector(&(0..=det_max).collect::<Vec<_>>())),
        n_spectra: det_max,
    };

    let nexus_structure = json!( {
        "children": [
            {
                "type": "group",
                "name": "raw_data_1",
                "children": [
                    {
                        "type": "group",
                        "name": "events",
                        "children": [
                            {
                                "type": "stream",
                                "stream": {
                                    "topic": event_topic,
                                    "source": "saluki_howl",
                                    "writer_module": "ev44",
                                },
                            },
                        ],
                        "attributes": [{"name": "NX_class", "values": "NXentry"}],
                    },
                ],
                "attributes": [{"name": "NX_class", "values": "NXentry"}],
            }
        ]
    });

    let det_spec_map_buf = SpectraDetectorMapping::create(fbb, &args);
    let file_name = Uuid::new_v4().to_string();
    let run_name = format!("saluki-howl-{}", Uuid::new_v4());

    let start_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_millis();

    let run_start_args = RunStartArgs {
        start_time: start_time as u64,
        stop_time: stop_time.unwrap_or_default(),
        run_name: Some(fbb.create_string(&run_name)),
        instrument_name: Some(fbb.create_string("saluki-howl")),
        nexus_structure: Some(fbb.create_string(&nexus_structure.to_string())),
        job_id: Some(fbb.create_string(job_id)),
        broker: None,
        service_id: None,
        filename: Some(fbb.create_string(&file_name)),
        n_periods: 1,
        detector_spectrum_map: Some(det_spec_map_buf),
        metadata: None,
        control_topic: None,
    };
    let run_start_buf = RunStart::create(fbb, &run_start_args);

    finish_run_start_buffer(fbb, run_start_buf);
    fbb.finished_data()
}

fn generate_run_stop<'a>(fbb: &'a mut FlatBufferBuilder<'_>, job_id: &str) -> &'a [u8] {
    fbb.reset();
    let stop_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_millis();

    let run_stop_args = RunStopArgs {
        stop_time: stop_time as u64,
        run_name: None,
        job_id: Some(fbb.create_string(job_id)),
        service_id: None,
        command_id: Some(fbb.create_string("")),
    };
    let run_stop_buf = RunStop::create(fbb, &run_stop_args);
    finish_run_stop_buffer(fbb, run_stop_buf);
    fbb.finished_data()
}

fn produce_messages(
    producer: &ThreadedProducer<DefaultProducerContext>,
    fbb: &mut FlatBufferBuilder,
    rng: &mut ThreadRng,
    frame: u32,
    conf: &HowlConfig,
    current_job_id: &mut String,
) {
    // get current time
    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_nanos()
        .try_into()
        .expect("This will fail after April 11th, 2262");

    match producer.send(
        BaseRecord::to(conf.event_topic)
            .key("")
            .payload(generate_fake_metadata(
                rng,
                fbb,
                now_nanos,
                conf.veto_probability,
            ))
            .timestamp(now_nanos / 1_000_000),
    ) {
        Ok(_) => {}
        Err(err) => {
            error!("Failed to send messages: {}", err.0);
        }
    }

    for _ in 0..conf.messages_per_frame {
        match producer.send(
            BaseRecord::to(conf.event_topic)
                .key("")
                .payload(generate_fake_events(
                    fbb,
                    rng,
                    frame,
                    conf.event_message_config,
                    now_nanos,
                ))
                .timestamp(now_nanos / 1_000_000),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send messages: {}", err.0);
            }
        }
    }

    if conf.frames_per_run > 0 && frame.is_multiple_of(conf.frames_per_run) {
        info!(
            "Starting new run after {} simulated frames",
            conf.frames_per_run
        );
        match producer.send(
            BaseRecord::to(conf.run_info_topic)
                .key("")
                .payload(generate_run_stop(fbb, current_job_id))
                .timestamp(now_nanos / 1_000_000),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send run stop: {}", err.0);
            }
        }
        *current_job_id = Uuid::new_v4().to_string();
        match producer.send(
            BaseRecord::to(conf.run_info_topic)
                .key("")
                .payload(generate_run_start(
                    fbb,
                    conf.event_message_config.det_max,
                    conf.event_topic,
                    current_job_id,
                    conf.stop_time,
                ))
                .timestamp(now_nanos / 1_000_000),
        ) {
            Ok(_) => {}
            Err(err) => {
                error!("Failed to send run start: {}", err.0);
            }
        }
    }
}

pub struct EventMessageConfig {
    pub events_per_message: i32,
    pub tof_peak: f32,
    pub tof_sigma: f32,
    pub det_min: i32,
    pub det_max: i32,
}

fn generate_fake_events<'a>(
    fbb: &'a mut FlatBufferBuilder<'_>,
    rng: &mut ThreadRng,
    msg_id: u32,
    conf: &EventMessageConfig,
    timestamp_ns: i64,
) -> &'a [u8] {
    fbb.reset();

    let det_ids: Vec<i32> = (0..conf.events_per_message)
        .map(|_| rng.random_range(conf.det_min..=conf.det_max))
        .collect();

    let normal =
        Normal::new(conf.tof_peak, conf.tof_sigma).expect("Failed to generate normal distribution");
    let tofs: Vec<i32> = (0..conf.events_per_message)
        .map(|_| normal.sample(rng) as i32)
        .collect();

    let args = Event44MessageArgs {
        source_name: Some(fbb.create_string("saluki")),
        message_id: msg_id as i64,
        reference_time: Some(fbb.create_vector(&[timestamp_ns])),
        reference_time_index: Some(fbb.create_vector(&[0])),
        time_of_flight: Some(fbb.create_vector(&tofs)),
        pixel_id: Some(fbb.create_vector(&det_ids)),
    };
    let ev44 = Event44Message::create(fbb, &args);
    finish_event_44_message_buffer(fbb, ev44);
    fbb.finished_data()
}

fn generate_fake_metadata<'a>(
    rng: &mut ThreadRng,
    fbb: &'a mut FlatBufferBuilder<'_>,
    timestamp_ns: i64,
    veto_probability: f64,
) -> &'a [u8] {
    fbb.reset();
    let is_vetoed = rng.random_range(0.0..1.0) < veto_probability;
    let args = Pu00MessageArgs {
        reference_time: timestamp_ns,
        message_id: 0,
        source_name: Some(fbb.create_string("saluki")),
        period_number: Some(0),
        vetos: Some(if is_vetoed { 1 } else { 0 }),
        proton_charge: Some(0.1),
    };
    let pu00 = Pu00Message::create(fbb, &args);
    finish_pu_00_message_buffer(fbb, pu00);
    fbb.finished_data()
}

pub struct HowlConfig<'a> {
    pub broker: &'a str,
    pub event_topic: &'a str,
    pub run_info_topic: &'a str,
    pub messages_per_frame: u32,
    pub frames_per_second: u32,
    pub frames_per_run: u32,
    pub stop_time: Option<u64>,
    pub veto_probability: f64, // 1 = always vetoed, 0 = never vetoed
    pub event_message_config: &'a EventMessageConfig,
    pub kafka_config: Option<Vec<KafkaOption>>,
}

pub fn howl(conf: &HowlConfig) {
    // create producer
    let mut fbb = FlatBufferBuilder::new();
    let mut rng = rand::rng();

    let now_nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time")
        .as_nanos()
        .try_into()
        .expect("This will fail after April 11th, 2262");

    let ev44_size =
        generate_fake_events(&mut fbb, &mut rng, 0, conf.event_message_config, now_nanos).len()
            as u32;
    debug!("ev44 size is {ev44_size} bytes");

    let pu00_size =
        generate_fake_metadata(&mut rng, &mut fbb, now_nanos, conf.veto_probability).len() as u32;
    debug!("pu00 size is {pu00_size} bytes");

    // calculate overall rate (with both ev44 and pu00)
    let rate_bytes_per_sec = ev44_size * conf.messages_per_frame * conf.frames_per_second
        + pu00_size * conf.frames_per_second;
    debug!("bytes per second: {rate_bytes_per_sec}");

    let rate_mbit_per_sec = (rate_bytes_per_sec as f64 / (1024. * 1024.)) * 8.0;
    let rate_mebibits_per_sec = rate_mbit_per_sec / 8.0;
    debug!("rate mbit per sec: {rate_mbit_per_sec}");
    println!(
        "Attempting to simulate data rate: {rate_mbit_per_sec:.3} Mbit/s ({rate_mebibits_per_sec:.3} MiB/s)"
    );
    println!("Each pu00 is {pu00_size} bytes");
    println!("Each ev44 is {ev44_size} bytes");

    let mut config: ClientConfig = ClientConfig::new();
    config.set("bootstrap.servers", conf.broker);

    if let Some(kafka_options) = &conf.kafka_config {
        for option in kafka_options {
            println!(
                "Setting Kafka config option {}={}",
                option.key, option.value
            );
            config.set(&option.key, &option.value);
        }
    }

    let producer: ThreadedProducer<DefaultProducerContext> =
        config.create().expect("Producer creation error");

    let mut current_job_id = Uuid::new_v4().to_string();

    producer
        .send(
            BaseRecord::to(conf.run_info_topic)
                .key("")
                .payload(generate_run_start(
                    &mut fbb,
                    conf.event_message_config.det_max,
                    conf.event_topic,
                    &current_job_id,
                    conf.stop_time,
                ))
                .timestamp(now_nanos / 1_000_000),
        )
        .expect("Failed to enqueue run start message");

    let target_frame_time = Duration::from_secs_f64(1.0 / conf.frames_per_second as f64);
    debug!("Target frame time: {target_frame_time:?}");

    let mut frames: u32 = 0;

    let mut target_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Failed to get system time");
    debug!("Target time: {target_time:?}");

    loop {
        target_time += target_frame_time;
        debug!("New target: {target_time:?}");
        frames += 1;
        debug!("current job id: {current_job_id}");
        produce_messages(
            &producer,
            &mut fbb,
            &mut rng,
            frames,
            conf,
            &mut current_job_id,
        );
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Failed to get system time");

        debug!("Current time: {now:?}");
        debug!("Target time: {target_time:?}");

        if target_time > now {
            let sleep_time = target_time - now;
            thread::sleep(sleep_time);
        } else {
            let behind = now - target_time;
            warn!(
                "saluki howl running {} ms behind schedule",
                behind.as_millis()
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use isis_streaming_data_types::flatbuffers_generated::run_start_pl72::root_as_run_start;

    fn serialised_stop_time(stop_time: Option<u64>) -> u64 {
        let mut fbb = FlatBufferBuilder::new();
        let data = generate_run_start(&mut fbb, 2, "events", "job", stop_time);
        root_as_run_start(data).unwrap().stop_time()
    }

    #[test]
    fn run_start_uses_configured_stop_time() {
        assert_eq!(serialised_stop_time(None), 0);
        assert_eq!(serialised_stop_time(Some(123456)), 123456);
    }
}
