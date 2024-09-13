use std::path::PathBuf;
use swh_graph::graph::*;
use swh_graph::properties::Maps;
use swh_graph::NodeType;

fn main()
{
    println!("starting program");
    let basename = PathBuf::from("../pythonCompressed/graph");

    println!("loading graph... this might take A WHILE (20 minutes for le petit teaser python)");
    let graph = swh_graph::graph::load_full::<swh_graph::mph::DynMphf>(basename)
        .expect("Could not load graph");
        println!("right here 3");
    println!("graph loaded !! :D");

    let origin_nodes: Vec<usize> = (0..graph.num_nodes())
    .filter(|&node| graph.properties().node_type(node) == NodeType::Origin)
    .collect();

    for node in origin_nodes {
        let swhid = graph.properties().swhid(node);
        println!("Origin node SWHID: {}", swhid);
    }
}
