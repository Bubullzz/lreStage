use std::env;
use std::path::PathBuf;
use std::fs::File;
use std::io::{Write, BufWriter};
use swh_graph::graph::*;
use swh_graph::NodeType;
use swh_graph::mph::DynMphf;
use indicatif::ProgressBar;
use base64::decode;

fn main() {
    // Retrieve the path from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <path_to_graph>", args[0]);
        std::process::exit(1);
    }
    let graph_path = PathBuf::from(&args[1]);

    println!("Loading graph from {:?}...", graph_path);
    let graph = swh_graph::graph::SwhUnidirectionalGraph::new(graph_path)
        .expect("Could not load graph")
        .init_properties()
        .load_properties(|properties| properties.load_maps::<DynMphf>())
        .expect("Could not load SWHID<->node id maps")
        .load_properties(|properties| properties.load_strings())
        .expect("Could not load strings");

    println!("Graph loaded!");
    println!("Starting to map nodes!");

    let origin_nodes: Vec<usize> = (0..graph.num_nodes())
        .filter(|&node| graph.properties().node_type(node) == NodeType::Origin)
        .collect();

    println!("Starting to iterate over the nodes!");

    // Create a file for output
    let output_file = File::create("output.txt").expect("Could not create output file");
    let mut writer = BufWriter::new(output_file);

    let pb = ProgressBar::new(origin_nodes.len() as u64);
    let word = "scm.gforge.inria";
    for node in origin_nodes {
        graph.properties().message_base64(node)
            .and_then(|data| decode(data).ok()) // Decode the Base64 string into bytes
            .and_then(|decoded_bytes| String::from_utf8(decoded_bytes).ok()) // Try to convert decoded bytes to a UTF-8 string
            .map(|string| {
                if string.contains(word) {
                    let result = format!("{}: {}\n", graph.properties().swhid(node), string);
                    writer.write_all(result.as_bytes()).expect("Could not write to file");
                }
            }).unwrap_or_else(|| {});
        pb.inc(1);
    }

    pb.finish_with_message("Processing complete!");
    println!("Results written to output.txt");
}
