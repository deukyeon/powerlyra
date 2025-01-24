/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/*
 * File:   distrubuted_degree_random_ingress.hpp
 * Author: vincent
 *
 * Created on July 19, 2017, 5:56 PM
 */

#ifndef DISTRUBUTED_DEGREE_RANDOM_INGRESS_HPP
#define DISTRUBUTED_DEGREE_RANDOM_INGRESS_HPP

#include <boost/functional/hash.hpp>

#include <graphlab/rpc/buffered_exchange.hpp>
#include <graphlab/graph/graph_basic_types.hpp>
#include <graphlab/graph/ingress/distributed_ingress_base.hpp>
#include <graphlab/graph/distributed_graph.hpp>

#include <graphlab/macros_def.hpp>
namespace graphlab {
template <typename VertexData, typename EdgeData>
class distributed_graph;

/**
 * \brief Ingress object assigning edges using randoming hash function on
 * vertex with higher degree between source and target
 */
template <typename VertexData, typename EdgeData>
class distributed_random_ingress
    : public distributed_ingress_base<VertexData, EdgeData> {
 public:
  typedef distributed_graph<VertexData, EdgeData> graph_type;
  /// The type of the vertex data stored in the graph
  typedef VertexData vertex_data_type;
  /// The type of the edge data stored in the graph
  typedef EdgeData edge_data_type;

  typedef distributed_ingress_base<VertexData, EdgeData> base_type;

  // Variables for partition alg
  std::vector<size_t> degree_vector;

 public:
  distributed_random_ingress(distributed_control& dc, graph_type& graph)
      : base_type(dc, graph) {}  // end of constructor

  ~distributed_random_ingress() {}

  /** Add an edge to the ingress object using random assignment. */
  void add_edge(vertex_id_type source, vertex_id_type target,
                const EdgeData& edata) {
    typedef typename base_type::edge_buffer_record edge_buffer_record;

    procid_t procid;
    if (degree_vector.at(source) > degree_vector.at(target))
      procid = graph_hash::hash_vertex(source) % base_type::rpc.numprocs();
    else
      graph_hash::hash_vertex(target) % base_type::rpc.numprocs();
    const procid_t owning_proc = procid;
    const edge_buffer_record record(source, target, edata);
    base_type::edge_exchange.send(owning_proc, record);
  }  // end of add edge
};   // end of distributed_random_ingress
};   // end of namespace graphlab
#include <graphlab/macros_undef.hpp>

#endif /* DISTRUBUTED_DEGREE_RANDOM_INGRESS_HPP */
