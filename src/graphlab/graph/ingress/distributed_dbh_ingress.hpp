/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   distrubuted_dbh_ingress.hpp
 * Author: Vincent
 *
 * Created on July 19, 2017, 5:56 PM
 */

#ifndef DISTRIBUTED_DBH_INGRESS_HPP
#define DISTRIBUTED_DBH_INGRESS_HPP

#include <boost/functional/hash.hpp>

#include <graphlab/rpc/buffered_exchange.hpp>
#include <graphlab/graph/graph_basic_types.hpp>
#include <graphlab/graph/ingress/distributed_ingress_base.hpp>
#include <graphlab/graph/distributed_graph.hpp>
#include <graphlab/logger/logger.hpp>
#include <vector>
#include <graphlab/graph/graph_hash.hpp>
#include <graphlab/rpc/distributed_event_log.hpp>

#include <graphlab/macros_def.hpp>
namespace graphlab {
template <typename VertexData, typename EdgeData>
class distributed_graph;

/**
 * \brief Ingress object assigning edges using random hash function on
 * vertex with higher degree between source and target
 */
template <typename VertexData, typename EdgeData>
class distributed_dbh_ingress
    : public distributed_ingress_base<VertexData, EdgeData> {
 public:
  typedef distributed_graph<VertexData, EdgeData> graph_type;
  /// The type of the vertex data stored in the graph
  typedef VertexData vertex_data_type;
  /// The type of the edge data stored in the graph
  typedef EdgeData edge_data_type;

  typedef distributed_ingress_base<VertexData, EdgeData> base_type;

  typedef typename base_type::edge_buffer_record edge_buffer_record;
  typedef typename buffered_exchange<edge_buffer_record>::buffer_type
      edge_buffer_type;

  graph_type& graph;

  typedef typename boost::unordered_map<vertex_id_type, size_t> degree_map_type;
  /// Variables for partition alg

  // store the degrees of each vertex
  degree_map_type degree_map;

  typedef typename std::pair<vertex_id_type, size_t> vertex_degree_pair;
  buffered_exchange<vertex_degree_pair> degree_exchange;
  typedef typename buffered_exchange<vertex_degree_pair>::buffer_type
      degree_buffer_type;

  // 1 edge exchange for first pass, when reading the edges to examine degree
  // the first edge exchange arbitrarily decides proc to send
  // second edge exchange used to add edge to correct proc owner,
  // that is hashing the higher degree vertex between source and target
  buffered_exchange<edge_buffer_record> dbh_edge_exchange;

  // rpc for this class, allows communication between objects through various
  // machines
  dc_dist_object<distributed_dbh_ingress> dbh_rpc;

 public:
  distributed_dbh_ingress(distributed_control& dc, graph_type& graph)
      : base_type(dc, graph),
        degree_exchange(dc),
        dbh_edge_exchange(dc),
        dbh_rpc(dc, this),
        graph(graph) {
    dbh_rpc.barrier();
  }  // end of constructor

  ~distributed_dbh_ingress() {}

  /** Add an edge to the ingress object using random assignment.
   * This is the first pass for format SNAP, used only to count the degree of
   * each vertex. The procid for the edge is arbitrary in this function, so
   * hashing the source will be used.
   *  */
  void add_edge(vertex_id_type source, vertex_id_type target,
                const EdgeData& edata) {
    procid_t procid;
    //    procid_t l_procid = dbh_rpc.procid();
    //    std::cout << "Reading " << source << " to " << target << " on machine
    //    " << l_procid << std::endl;

    // increase degree of source
    // degrees are stored/accumulated on each machine locally, then sent out in
    // finalize
    if (degree_map.find(source) != degree_map.end()) {
      ++degree_map[source];
    } else
      degree_map.emplace(source, 1);

    /*
          typename degree_map_type::iterator degree_it =
       degree_map.insert(std::make_pair(source, 0)).first; degree_it->second =
       degree_it->second + 1;
      */
    procid = graph_hash::hash_vertex(source) % dbh_rpc.numprocs();
    const procid_t owning_proc = procid;
    const edge_buffer_record record(source, target, edata);
    dbh_edge_exchange.send(owning_proc, record);
  }  // end of add edge

  /** Assign the edges from the first passthrough of add_edges to its rightful
   * procid Add an edge to the ingress object by hashing the source or target
   * vid chosen by taking the vertex with higher degree, ties broken by taking
   * source
   *  */
  void assign_edges() {
    graphlab::timer ti;
    size_t nprocs = dbh_rpc.numprocs();
    procid_t l_procid = dbh_rpc.procid();

    edge_buffer_type edge_buffer;
    procid_t proc = -1;
    while (dbh_edge_exchange.recv(proc, edge_buffer)) {
      for (typename edge_buffer_type::iterator it = edge_buffer.begin();
           it != edge_buffer.end(); ++it) {
        procid_t procid;
        size_t source_degree = 0;
        size_t target_degree = 0;
        if (degree_map.find(it->source) != degree_map.end())
          source_degree = degree_map.at(it->source);
        if (degree_map.find(it->target) != degree_map.end())
          target_degree = degree_map.at(it->target);
        if (source_degree < target_degree)
          procid = graph_hash::hash_vertex(it->source) % dbh_rpc.numprocs();
        else
          procid = graph_hash::hash_vertex(it->target) % dbh_rpc.numprocs();

        const procid_t owning_proc = procid;
        const edge_buffer_record record(it->source, it->target, it->edata);
        base_type::edge_exchange.send(owning_proc, record);
      }
    }
    std::cout << "Finished Assigning Edges" << std::endl;
    dbh_rpc.barrier();
    dbh_edge_exchange.clear();
  }  // end of assign_edges

  // finalize will need to combine the degree exchanges
  void finalize() {
    graphlab::timer ti;

    size_t nprocs = dbh_rpc.numprocs();
    procid_t l_procid = dbh_rpc.procid();

    dbh_rpc.full_barrier();

    if (l_procid == 0) {
      memory_info::log_usage("start finalizing");
      logstream(LOG_EMPH) << "DBH finalizing ..."
                          << " #vertices=" << graph.local_graph.num_vertices()
                          << " #edges=" << graph.local_graph.num_edges()
                          << std::endl;
    }
    /**************************************************************************/
    /*                                                                        */
    /*                        Flush additional data                           */
    /*                                                                        */
    /**************************************************************************/

    dbh_edge_exchange.flush();
    base_type::edge_exchange.flush();

    /**
     * Fast pass for redundant finalization with no graph changes.
     */
    {
      size_t changed_size =
          base_type::edge_exchange.size() + dbh_edge_exchange.size();
      dbh_rpc.all_reduce(changed_size);
      if (changed_size == 0) {
        logstream(LOG_INFO) << "Skipping Graph Finalization because no changes "
                               "happened... (pass 1)"
                            << std::endl;
        return;
      }
    }

    /**************************************************************************/
    /*                                                                        */
    /*                       Manage all degree values                        */
    /*                                                                        */
    /**************************************************************************/
    if (nprocs != 1) {
      if (l_procid == 0)
        logstream(LOG_INFO) << "Collecting Degree Counts" << std::endl;
      std::cout << "sending degrees to main" << std::endl;
      // send degree_vector values to main machine
      if (l_procid != 0) {
        // send the degree_vector values into the exchange so main computer can
        // handle
        for (typename degree_map_type::iterator it = degree_map.begin();
             it != degree_map.end(); ++it) {
          degree_exchange.send(0, std::make_pair(it->first, it->second));
        }
      }
      dbh_rpc.full_barrier();
      degree_exchange.flush();
      //	    std::cout << "summing degrees on main" << std::endl;
      // sum degree values from other machines
      if (l_procid == 0) {
        std::cout << "summing degrees on main" << std::endl;
        for (procid_t procid = 1; procid < nprocs; procid++) {
          std::cout << "summing form procid: " << procid << std::endl;
          // for each procid, get the contents of its degree exchange
          degree_buffer_type degree_accum;
          while (degree_exchange.recv(procid, degree_accum)) {
            for (typename degree_buffer_type::iterator it =
                     degree_accum.begin();
                 it != degree_accum.end(); ++it) {
              if (degree_map.find(it->first) == degree_map.end()) {
                degree_map.emplace(it->first, it->second);
              } else
                degree_map.at(it->first) =
                    degree_map.at(it->first) + it->second;
            }
          }
        }
        degree_exchange.clear();
        std::cout << "send degrees from main" << std::endl;
        // send the final degree vector to the other machines
        for (procid_t procid = 1; procid < nprocs; ++procid) {
          std::cout << "sending to procid: " << procid << std::endl;
          for (typename degree_map_type::iterator it = degree_map.begin();
               it != degree_map.end(); ++it) {
            degree_exchange.send(procid, std::make_pair(it->first, it->second));
          }
        }
      }
      dbh_rpc.barrier();
      degree_exchange.flush();

      // update degree map for sub machines
      if (l_procid != 0) {
        std::cout << "Updating degree on machine " << l_procid << std::endl;
        procid_t rec_proc = 0;
        degree_buffer_type rec_degree_map;
        while (degree_exchange.recv(rec_proc, rec_degree_map)) {
          for (typename degree_buffer_type::iterator it =
                   rec_degree_map.begin();
               it != rec_degree_map.end(); ++it) {
            if (degree_map.find(it->first) == degree_map.end()) {
              degree_map.emplace(it->first, it->second);
            } else
              degree_map.at(it->first) = it->second;
          }
        }
      }
      dbh_rpc.barrier();
      degree_exchange.clear();
    }

    /**************************************************************************/
    /*                                                                        */
    /*                       Assign Edges                                     */
    /*                                                                        */
    /**************************************************************************/
    assign_edges();
    if (l_procid == 0) {
      logstream(LOG_INFO) << "assign edges: " << ti.current_time() << " secs"
                          << std::endl;
    }

    dbh_rpc.barrier();
    base_type::finalize();

  }  // end of finalize
};   // end of distributed_dbh_ingress
};   // end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif /* DISTRIBUTED_DBH_INGRESS_HPP */
