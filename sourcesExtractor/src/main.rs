use std::path::PathBuf;
use swh_graph::graph::*;
use swh_graph::NodeType;
use swh_graph::mph::DynMphf;

fn main()
{
    println!("starting program");
    println!("loading graph...");
    let graph = swh_graph::graph::SwhUnidirectionalGraph::new(PathBuf::from("/masto/2024-05-16/compressed/graph"))
    .expect("Could not load graph")
    .init_properties()
    .load_properties(|properties| properties.load_maps::<DynMphf>())
    .expect("Could not load SWHID<->node id maps");

    println!("graph loaded !! :D");
    println!("starting to map !! :D");


    let origin_nodes: Vec<usize> = (0..graph.num_nodes())
    .filter(|&node| graph.properties().node_type(node) == NodeType::Origin)
    .collect();

    println!("starting to print !! :D");


    for node in origin_nodes {
        let swhid = graph.properties().swhid(node);
        println!("{}", swhid);
    }
}
