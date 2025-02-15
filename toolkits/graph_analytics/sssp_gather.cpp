/**
 * Copyright (c) 2009 Carnegie Mellon University.
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */

#include <vector>
#include <string>
#include <fstream>

#include <graphlab.hpp>

/**
 * \brief The type used to measure distances in the graph.
 */
typedef float distance_type;

/**
 * \brief The current distance of the vertex.
 */
struct vertex_data : graphlab::IS_POD_TYPE {
  distance_type dist;
  vertex_data(distance_type dist = std::numeric_limits<distance_type>::max())
      : dist(dist) {}
};  // end of vertex data

/**
 * \brief The distance associated with the edge.
 */
struct edge_data : graphlab::IS_POD_TYPE {
  distance_type dist;
  edge_data(distance_type dist = 1) : dist(dist) {}
};  // end of edge data

/**
 * \brief The graph type encodes the distances between vertices and
 * edges
 */
typedef graphlab::distributed_graph<vertex_data, edge_data> graph_type;

/**
 * \brief Get the other vertex in the edge.
 */
inline graph_type::vertex_type get_other_vertex(
    const graph_type::edge_type& edge, const graph_type::vertex_type& vertex) {
  return vertex.id() == edge.source().id() ? edge.target() : edge.source();
}

/**
 * \brief Use directed or undireced edges.
 */
bool DIRECTED_SSSP = false;

/**
 * \brief This class is used as the gather type.
 */
struct min_distance_type : graphlab::IS_POD_TYPE {
  distance_type dist;
  min_distance_type(
      distance_type dist = std::numeric_limits<distance_type>::max())
      : dist(dist) {}
  min_distance_type& operator+=(const min_distance_type& other) {
    dist = std::min(dist, other.dist);
    return *this;
  }
};

struct max_distance_type : graphlab::IS_POD_TYPE {
  distance_type dist;
  max_distance_type(
      distance_type dist = std::numeric_limits<distance_type>::min())
      : dist(dist) {}
  max_distance_type& operator+=(const max_distance_type& other) {
    dist = std::max(dist, other.dist);
    return *this;
  }
};

/**
 * \brief The single source shortest path vertex program.
 */
class sssp_gather
    : public graphlab::ivertex_program<graph_type, min_distance_type>,
      public graphlab::IS_POD_TYPE {
  distance_type min_dist;
  bool changed;

 public:
  /**
   * \brief We use the messaging model to compute the SSSP update
   */
  edge_dir_type gather_edges(icontext_type& context,
                             const vertex_type& vertex) const {
    return DIRECTED_SSSP ? graphlab::IN_EDGES : graphlab::ALL_EDGES;
  };  // end of gather_edges

  /**
   * \brief Collect the distance to the neighbor
   */
  min_distance_type gather(icontext_type& context, const vertex_type& vertex,
                           edge_type& edge) const {
    return min_distance_type(1 + get_other_vertex(edge, vertex).data().dist);
  }  // end of gather function

  /**
   * \brief If the distance is smaller then update
   */
  void apply(icontext_type& context, vertex_type& vertex,
             const min_distance_type& total) {
    changed = false;
    if (vertex.data().dist > total.dist) {
      changed = true;
      vertex.data().dist = total.dist;
    }

    // activate neighbours at first iteration no matter what
    if (context.iteration() == 0) {
      vertex.data().dist = 0;
      changed = true;
    }
  }

  /**
   * \brief Determine if SSSP should run on all edges or just in edges
   */
  edge_dir_type scatter_edges(icontext_type& context,
                              const vertex_type& vertex) const {
    if (changed)
      return DIRECTED_SSSP ? graphlab::OUT_EDGES : graphlab::ALL_EDGES;
    else
      return graphlab::NO_EDGES;
  };  // end of scatter_edges

  /**
   * \brief The scatter function just signal adjacent pages
   */
  void scatter(icontext_type& context, const vertex_type& vertex,
               edge_type& edge) const {
    // scatter_edges return NO_EDGES in case there is no change
    // so no need to check again. If changed, signal all neighbours
    const vertex_type other = get_other_vertex(edge, vertex);
    context.signal(other);
  }  // end of scatter

};  // end of shortest path vertex program

/**
 * \brief We want to save the final graph so we define a write which will be
 * used in graph.save("path/prefix", pagerank_writer()) to save the graph.
 */
struct shortest_path_writer {
  std::string save_vertex(const graph_type::vertex_type& vtx) {
    std::stringstream strm;
    if (vtx.data().dist != std::numeric_limits<distance_type>::max())
      strm << vtx.id() << "\t" << vtx.data().dist << "\n";
    return strm.str();
  }
  std::string save_edge(graph_type::edge_type e) { return ""; }
};  // end of shortest_path_writer

struct max_deg_vertex_reducer : public graphlab::IS_POD_TYPE {
  size_t degree;
  graphlab::vertex_id_type vid;
  max_deg_vertex_reducer& operator+=(const max_deg_vertex_reducer& other) {
    if (degree < other.degree) {
      (*this) = other;
    }
    return (*this);
  }
};

max_deg_vertex_reducer find_max_deg_vertex(const graph_type::vertex_type vtx) {
  max_deg_vertex_reducer red;
  red.degree = vtx.num_in_edges() + vtx.num_out_edges();
  red.vid = vtx.id();
  return red;
}

max_distance_type map_dist(const graph_type::vertex_type& v) {
  if (v.data().dist == std::numeric_limits<distance_type>::max())
    return std::numeric_limits<distance_type>::min();

  max_distance_type dist(v.data().dist);
  return dist;
}

int main(int argc, char** argv) {
  // Initialize control plain using mpi
  graphlab::mpi_tools::init(argc, argv);
  graphlab::distributed_control dc;
  global_logger().set_log_level(LOG_INFO);

  // Parse command line options -----------------------------------------------
  graphlab::command_line_options clopts(
      "Single Source Shortest Path Algorithm.");
  std::string graph_dir;
  std::string format = "adj";
  std::string exec_type = "synchronous";
  size_t powerlaw = 0;
  std::vector<unsigned int> sources;
  bool max_degree_source = false;
  clopts.attach_option("graph", graph_dir,
                       "The graph file.  If none is provided "
                       "then a toy graph will be created");
  clopts.add_positional("graph");
  clopts.attach_option("format", format, "graph format");
  clopts.attach_option("source", sources, "The source vertices");
  clopts.attach_option("max_degree_source", max_degree_source,
                       "Add the vertex with maximum degree as a source");

  clopts.add_positional("source");

  clopts.attach_option("directed", DIRECTED_SSSP, "Treat edges as directed.");

  clopts.attach_option("engine", exec_type,
                       "The engine type synchronous or asynchronous");

  clopts.attach_option("powerlaw", powerlaw,
                       "Generate a synthetic powerlaw out-degree graph. ");
  std::string saveprefix;
  clopts.attach_option("saveprefix", saveprefix,
                       "If set, will save the resultant pagerank to a "
                       "sequence of files with prefix saveprefix");

  if (!clopts.parse(argc, argv)) {
    dc.cout() << "Error in parsing command line arguments." << std::endl;
    return EXIT_FAILURE;
  }

  // Build the graph ----------------------------------------------------------
  dc.cout() << "Loading graph." << std::endl;
  graphlab::timer timer;
  graph_type graph(dc, clopts);
  if (powerlaw > 0) {  // make a synthetic graph
    dc.cout() << "Loading synthetic Powerlaw graph." << std::endl;
    graph.load_synthetic_powerlaw(powerlaw, false, 2, 100000000);
  } else if (graph_dir.length() > 0) {  // Load the graph from a file
    dc.cout() << "Loading graph in format: " << format << std::endl;
    graph.load_format(graph_dir, format);
  } else {
    dc.cout() << "graph or powerlaw option must be specified" << std::endl;
    clopts.print_description();
    return EXIT_FAILURE;
  }
  const double loading = timer.current_time();
  dc.cout() << "Loading graph. Finished in " << loading << std::endl;

  // must call finalize before querying the graph
  dc.cout() << "Finalizing graph." << std::endl;
  timer.start();
  graph.finalize();
  const double finalizing = timer.current_time();
  dc.cout() << "Finalizing graph. Finished in " << finalizing << std::endl;

  // NOTE: ingress time = loading time + finalizing time
  const double ingress = loading + finalizing;
  dc.cout() << "Final Ingress (second): " << ingress << std::endl;

  dc.cout() << "Final Ingress (second): " << ingress << std::endl;

  dc.cout() << "#vertices: " << graph.num_vertices()
            << " #edges:" << graph.num_edges() << std::endl;

  dc.cout() << "Sources : ";
  for (std::vector<unsigned int>::iterator it = sources.begin();
       it != sources.end(); ++it) {
    dc.cout() << *it << " ";
  }
  dc.cout() << std::endl;
  if (sources.empty()) {
    if (max_degree_source == false) {
      dc.cout() << "No source vertex provided. Adding vertex 0 as source"
                << std::endl;
      sources.push_back(0);
    }
  }

  if (max_degree_source) {
    max_deg_vertex_reducer v =
        graph.map_reduce_vertices<max_deg_vertex_reducer>(find_max_deg_vertex);
    dc.cout() << "No source vertex provided.  Using highest degree vertex "
              << v.vid << " as source." << std::endl;
    sources.push_back(v.vid);
  }

  // Running The Engine -------------------------------------------------------
  graphlab::omni_engine<sssp_gather> engine(dc, graph, exec_type, clopts);

  // Signal all the vertices in the source set
  for (size_t i = 0; i < sources.size(); ++i) {
    dc.cout() << "Using Source " << sources[i] << std::endl;
    engine.signal(sources[i]);
  }

  timer.start();
  engine.start();
  const double runtime = timer.current_time();
  dc.cout() << "----------------------------------------------------------"
            << std::endl
            << "Final Runtime (seconds):   " << runtime << std::endl
            << "Updates executed: " << engine.num_updates() << std::endl
            << "Update Rate (updates/second): "
            << engine.num_updates() / runtime << std::endl;

  const max_distance_type max_dist =
      graph.map_reduce_vertices<max_distance_type>(map_dist);
  std::cout << "Max distance: " << max_dist.dist << std::endl;

  // Save the final graph -----------------------------------------------------
  if (saveprefix != "") {
    graph.save(saveprefix, shortest_path_writer(),
               false,   // do not gzip
               true,    // save vertices
               false);  // do not save edges
  }

  // Tear-down communication layer and quit -----------------------------------
  graphlab::mpi_tools::finalize();
  return EXIT_SUCCESS;
}  // End of main

// We render this entire program in the documentation
