package backend

type NodeType int

const (
	NodeInternal NodeType = iota
	NodeLeaf
)

// Common Node Header Layout
const (
	NodeTypeSize uint32 = 1
	NodeTypeOffset uint32 = 0
	IsRootSize uint32 = 1
	IsRootOffset = NodeTypeSize
	ParentPointerSize = 4
	ParentPointerOffset = IsRootOffset + IsRootSize
	CommonNodeHeaderSize = NodeTypeSize + IsRootSize + ParentPointerSize
)

// Leaf Node Format
const (
	LeafNodeNumCellsSize uint32 = 4
	LeafNodeNumCellsOffset uint32 = CommonNodeHeaderSize
	LeafNodeHeaderSize uint32 = CommonNodeHeaderSize + LeafNodeNumCellsSize
)

// Leaf Node Body Layout
const (
	LeafNodeKeySize uint32 = 4
	LeafNodeKeyOffset uint32 = 0
	LeafNodeValueSize uint32 = RowSize
	LeafNodeValueOffset uint32 = LeafNodeKeyOffset + LeafNodeKeySize
	LeafNodeCellSize uint32 = LeafNodeKeySize + LeafNodeValueSize
	LeafNodeSpaceForCells uint32 = PageSize - LeafNodeHeaderSize
	LeafNodeMaxCells uint32 = LeafNodeSpaceForCells / LeafNodeCellSize
)
