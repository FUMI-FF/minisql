package backend

import (
	"encoding/binary"
	"errors"
)

type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

// Common Node Header Layout
const (
	NodeTypeSize         uint32 = 1
	NodeTypeOffset       uint32 = 0
	IsRootSize           uint32 = 1
	IsRootOffset                = NodeTypeSize
	ParentPointerSize           = 4
	ParentPointerOffset         = IsRootOffset + IsRootSize
	CommonNodeHeaderSize        = NodeTypeSize + IsRootSize + ParentPointerSize
)

// LeafNode
type LeafNode struct {
	data []byte
}

// Leaf Node Format
const (
	LeafNodeNumCellsSize   uint32 = 4
	LeafNodeNumCellsOffset uint32 = CommonNodeHeaderSize
	LeafNodeHeaderSize     uint32 = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

// Leaf Node Body Layout
const (
	LeafNodeKeySize       uint32 = 4
	LeafNodeKeyOffset     uint32 = 0
	LeafNodeValueSize     uint32 = RowSize
	LeafNodeValueOffset   uint32 = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize      uint32 = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells uint32 = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells      uint32 = LeafNodeSpaceForCells / LeafNodeCellSize
)

func NewLeafNode(b []byte) *LeafNode {
	return &LeafNode{data: b}
}

func (n *LeafNode) NumberOfCells() uint32 {
	return binary.LittleEndian.Uint32(n.data[LeafNodeNumCellsOffset:])
}

func (n *LeafNode) CellOffset(cellNum uint32) int {
	return int(LeafNodeHeaderSize) + int(LeafNodeCellSize)*int(cellNum)
}

func (n *LeafNode) Key(cellNum uint32) uint32 {
	offset := n.CellOffset(cellNum)
	return binary.LittleEndian.Uint32(n.data[offset:])
}

func (n *LeafNode) Value(cellNum uint32) []byte {
	offset := n.CellOffset(cellNum)
	return n.data[offset : offset+int(LeafNodeValueSize)]
}

func (n *LeafNode) Insert(cellNum uint32, key uint32, value []byte) error {
	if cellNum > LeafNodeMaxCells {
		return errors.New("splitting Node is not implemented")
	}

	numOfCells := n.NumberOfCells()

	if cellNum < numOfCells {
		// make room for insert
		for i := numOfCells; i > cellNum; i-- {
			dst := n.CellOffset(i)
			src := n.CellOffset(i - 1)
			copy(n.data[dst:dst+int(LeafNodeCellSize)], n.data[src:src+int(LeafNodeCellSize)])
		}
	}

	// TODO: write +1 NumberOfCells, insert key & value

	return nil
}
