package backend

import (
	"errors"
	"os"
	"syscall"
	"io"
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
		return nil, errors.New("tried to fetch page number out of bound")
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
			_, err := p.file.ReadAt(page, int64(pageNum)*PageSize)
			if errors.Is(err, io.EOF) {
				return page, nil
			} else if err != nil {
				return nil, err
			}
		}

		p.pages[pageNum] = page
	}
	return p.pages[pageNum], nil
}

func (p *Pager) flush(pageNum uint32, bytesToWrite uint32) error {
	if p.pages[pageNum] == nil {
		return errors.New("tried to flush null page")
	}
	if bytesToWrite == 0 || bytesToWrite > PageSize {
		return errors.New("invalid bytesToWrite")
	}

	offset := int64(pageNum) * int64(PageSize)
	_, err := p.file.WriteAt(p.pages[pageNum][:bytesToWrite], offset)
	if err != nil {
		return err
	}

	return nil
}
