package backend

import (
	"errors"
	"io"
	"os"
	"syscall"
)

var (
	ErrPagerPageOutOfBound   = errors.New("tried to fetch page number out of bound")
	ErrPagerNullPageFlush    = errors.New("tried to flush null page")
	ErrPagerInvalidSizeFlush = errors.New("tried to flush invalid size page")
)

type Pager struct {
	file       *os.File
	fileLength uint32
	pages      [TableMaxPages][]byte
}

func newPage(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, syscall.S_IWUSR|syscall.S_IRUSR)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &Pager{
		file:       file,
		fileLength: uint32(info.Size()),
	}, nil
}

func (p *Pager) getPage(pageNum uint32) ([]byte, error) {
	if pageNum > TableMaxPages {
		return nil, ErrPagerPageOutOfBound
	}
	if p.pages[pageNum] == nil {
		// cache miss. allocate memory and load file
		page := make([]byte, PageSize)
		numPages := p.fileLength / PageSize

		// we might save a partial page at the end of the file
		if (p.fileLength % PageSize) > 0 {
			numPages += 1
		}

		if pageNum < numPages {
			_, err := p.file.ReadAt(page, int64(pageNum)*int64(PageSize))
			// ignore io.EOF  
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
		}

		p.pages[pageNum] = page
	}
	return p.pages[pageNum], nil
}

func (p *Pager) flush(pageNum uint32, bytesToWrite uint32) error {
	if p.pages[pageNum] == nil {
		return ErrPagerNullPageFlush
	}
	if bytesToWrite == 0 || bytesToWrite > PageSize {
		return ErrPagerInvalidSizeFlush
	}

	offset := int64(pageNum) * int64(PageSize)
	_, err := p.file.WriteAt(p.pages[pageNum][:bytesToWrite], offset)
	if err != nil {
		return err
	}

	return nil
}
