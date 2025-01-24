#!/usr/bin/python

import sys
import os

if len(sys.argv) < 4:
	print "[file_name] [output-dir] [# of partitions] [edge_balanced] "

inputFile	= sys.argv[1]
outputDir 	= sys.argv[2]
nparts 		= int(sys.argv[3])

tot_verts	= 0
tot_edges	= 0

# create output dir
import os
if not os.path.exists(outputDir):
    os.makedirs(outputDir)

metisGraph = open(inputFile, 'rb')
line = metisGraph.readline().split()
tot_verts = int(line[0])
tot_edges = int(line[1])

# create the dist array
vertDist = [0] * (nparts + 1)
vertDist[nparts] = tot_verts
tmpLocation = 0
for i in range(nparts):
	vertDist[i] = tmpLocation
	tmpLocation += int(tot_verts / nparts)

# write xdist array into a file
xdistFile = open(os.path.join(outputDir, "vtxdist"), 'w')
xdistFile.write(' '.join(map(str,vertDist)))

xdistFile.close()

# start reading the file for xadj and adjcny
for partition in range(nparts):
	# create the files
	xadjFile = open(os.path.join(outputDir, "xadj_" + str(partition) ), 'w')
	adjncyFile = open(os.path.join(outputDir, "adjncy_" + str(partition) ), 'w')
	vwgtFile = open(os.path.join(outputDir, "vwgt_" + str(partition) ), 'w')
	currentVertex = 0
	currentEdge = 0

	for i in range(vertDist[partition], vertDist[partition + 1]):
		vertexList = map(long, metisGraph.readline().split())
		# we need to remove 1 as parmetis accepts c style lists
		neighbours = [x-1 for x in vertexList]
		xadjFile.write(str(currentEdge) + " ")
		vwgtFile.write(str(len(neighbours)) + " ")
		currentEdge += len(neighbours)
		adjncyFile.write(" ".join(map(str, neighbours)) + " ")
		currentVertex += 1

	# write last edge count, it will be ignored by parmetis
	xadjFile.write(str(currentEdge))
	xadjFile.close()
	adjncyFile.close()
	vwgtFile.close()

# we can close the input file
metisGraph.close()
