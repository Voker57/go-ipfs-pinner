// Package pin implements structures and methods to keep track of
// which objects a user wants to keep stored locally.
package pin

import (
	"context"
	"fmt"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	dsq "github.com/ipfs/go-datastore/query"
	mdag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-merkledag/dagutils"
)

var log = logging.Logger("pin")

var pinDatastoreKey = "/local/pins"

// Mode allows to specify different types of pin (recursive, direct etc.).
// See the Pin Modes constants for a full list.
type Mode int

// Pin Modes
const (
	// Recursive pins pin the target cids along with any reachable children.
	Recursive Mode = iota

	// Direct pins pin just the target cid.
	Direct

	// Indirect pins are cids who have some ancestor pinned recursively.
	Indirect

	// NotPinned
	NotPinned
)

// A Pinner provides the necessary methods to keep track of Nodes which are
// to be kept locally, according to a pin mode. In practice, a Pinner is in
// in charge of keeping the list of items from the local storage that should
// not be garbage-collected.
type Pinner interface {
	// IsPinned returns whether or not the given cid is pinned
	// and an explanation of why its pinned
	IsPinned(ctx context.Context, c cid.Cid) (string, bool, error)

	// GetPin returns cid pinned at given path
	GetPin(path string, recursive bool) (*cid.Cid, error)

	// Pin the given node, optionally recursively.
	Pin(ctx context.Context, path string, node ipld.Node, recursive bool) error

	// Unpin the given path with given mode.
	Unpin(path string, recursive bool) error
	
	// Unpin the given cid with given mode.
	UnpinCid(cid cid.Cid, recursive bool) error

	UnpinCidUnderPrefix(path string, c cid.Cid, recursive bool) error

	// Update updates a recursive pin from one cid to another
	// this is more efficient than simply pinning the new one and unpinning the
	// old one
	Update(ctx context.Context, path string, to cid.Cid) error

	// Check if given cid is pinned with given mode, return pin path
	IsPinPresent(cid cid.Cid, recursive bool) (string, error)

	// Check if a set of keys are pinned, more efficient than
	// calling IsPinned for each key
	CheckIfPinned(ctx context.Context, cids ...cid.Cid) ([]Pinned, error)

	// AddPin is for manually editing the pin structure. Use with
	// care! If used improperly, garbage collection may not be
	// successful.
	AddPin(path string, cid cid.Cid, recursive bool) error

	// PinnedCids returns all pinned cids (recursive or direct)
	PinnedCids(recursive bool) ([]cid.Cid, error)

	PrefixedPins(prefix string, recursive bool) (map[string]cid.Cid, error)
}

// Pinned represents CID which has been pinned with a pinning strategy.
// The Via field allows to identify the pinning parent of this CID, in the
// case that the item is not pinned directly (but rather pinned recursively
// by some ascendant).
type Pinned struct {
	Key  cid.Cid
	Mode Mode
	Path string
	Via  cid.Cid
}

// Pinned returns whether or not the given cid is pinned
func (p Pinned) Pinned() bool {
	return p.Mode != NotPinned
}

// String Returns pin status as string
func (p Pinned) String() string {
	var result string
	switch p.Mode {
	case NotPinned:
		result = "not pinned"
	case Indirect:
		result = fmt.Sprintf("pinned under %s via %s", p.Path, p.Via)
	case Recursive:
		result = fmt.Sprintf("pinned under %s recursively", p.Path)
	case Direct:
		result = fmt.Sprintf("pinned under %s directly", p.Path)
	}
	return result
}

// pinner implements the Pinner interface
type pinner struct {
	dserv  ipld.DAGService
	dstore ds.Datastore
}

// NewPinner creates a new pinner using the given datastore as a backend
func NewPinner(dserv ipld.DAGService, dstore ds.Datastore) Pinner {
	return &pinner{dserv: dserv, dstore: dstore}
}

// Pin the given node, optionally recursive
// If path ends with /, cid will be appended to path.
func (p *pinner) AddPin(path string, c cid.Cid, recursive bool) error {
	if len(path) == 0 || path[len(path)-1] == '/' {
		path += c.String()
	}
	return p.dstore.Put(ds.NewKey(pathToDSKey(path, recursive)), c.Bytes())
}

// Pin the given node, optionally recursive
// Also fetches its data
// If path ends with /, cid will be appended to path.
func (p *pinner) Pin(ctx context.Context, path string, node ipld.Node, recurse bool) error {
	err := p.dserv.Add(ctx, node)
	if err != nil {
		return err
	}

	c := node.Cid()

	if recurse {

		err := mdag.FetchGraph(ctx, c, p.dserv)
		if err != nil {
			return err
		}
	} else {
		_, err := p.dserv.Get(ctx, c)
		if err != nil {
			return err
		}
	}

	return p.AddPin(path, c, recurse)
}

func (p *pinner) GetPin(path string, recursive bool) (*cid.Cid, error) {
	bytes, err := p.dstore.Get(ds.NewKey(pathToDSKey(path, recursive)))
	if err == ds.ErrNotFound {
		return nil, ErrNotPinned
	} else if err != nil {
		return nil, err
	}

	cid, err := cid.Cast(bytes)
	if err != nil {
		return nil, err
	}
	return &cid, nil
}

// ErrNotPinned is returned when trying to unpin items which are not pinned.
var ErrNotPinned = fmt.Errorf("not pinned or pinned indirectly")

// Unpin a given key
func (p *pinner) Unpin(path string, recursive bool) error {
	_, err := p.GetPin(path, recursive)
	if err != nil {
		return err
	}

	return p.dstore.Delete(ds.NewKey(pathToDSKey(path, recursive)))
}

// Unpin a given cid pinned under given prefix
// e.g UCUP("/temp/", QmAFobaz... will unpin /temp/QmAFobaz
// If path does not end in a slash, just acts as Unpin()
func (p *pinner) UnpinCidUnderPrefix(path string, c cid.Cid, recursive bool) error {
	if path[len(path)-1] == '/' {
		path += c.String()
	}
	_, err := p.GetPin(path, recursive)
	if err != nil {
		return err
	}

	return p.dstore.Delete(ds.NewKey(pathToDSKey(path, recursive)))
}

func pinKeyPrefix(recursive bool) string {
	prefix := "direct"
	if recursive {
		prefix = "recursive"
	}
	return pinDatastoreKey + "/" + prefix + "/"
}
func pathToDSKey(path string, recursive bool) string {
	return pinKeyPrefix(recursive) + path
}

func (p *pinner) IsPinPresent(cid cid.Cid, recursive bool) (string, error) {
	pinMap, err := p.PrefixedPins("", recursive)
	if err != nil {
		return "", err
	}
	for path, c := range pinMap {
		if c == cid {
			return path, nil
		}
	}
	return "", ErrNotPinned
}

func (p *pinner) IsPinned(ctx context.Context, cid cid.Cid) (string, bool, error) {
	pinneds, err := p.CheckIfPinned(ctx, cid)
	if err != nil {
		return "", false, err
	}
	if len(pinneds) != 1 {
		return "", false, fmt.Errorf("CheckIfPinned returned %d results instead of 1", len(pinneds))
	}
	return pinneds[0].String(), pinneds[0].Pinned(), nil
}

// CheckIfPinned Checks if a set of keys are pinned, more efficient than
// calling IsPinned for each key, returns the pinned status of cid(s)
func (p *pinner) CheckIfPinned(ctx context.Context, cids ...cid.Cid) ([]Pinned, error) {
	recursiveMap, err := p.PrefixedPins("", true)
	if err != nil {
		return nil, err
	}
	directMap, err := p.PrefixedPins("", false)
	if err != nil {
		return nil, err
	}
	pinned := make([]Pinned, 0, len(cids))
	toCheck := cid.NewSet()

	for _, c := range cids {
		toCheck.Visit(c)
	}

	if toCheck.Len() == 0 {
		return pinned, nil
	}

	for path, c := range directMap {
		if toCheck.Has(c) {
			pinned = append(pinned,
				Pinned{Key: c, Mode: Direct, Path: path})
			toCheck.Remove(c)
		}
	}

	if toCheck.Len() == 0 {
		return pinned, nil
	}

	// Now walk all recursive pins to check for indirect pins
	var checkChildren func(string, cid.Cid, cid.Cid) error
	checkChildren = func(path string, rk, parentKey cid.Cid) error {
		links, err := ipld.GetLinks(ctx, p.dserv, parentKey)
		if err != nil {
			return err
		}
		for _, lnk := range links {
			c := lnk.Cid

			if toCheck.Has(c) {
				pinned = append(pinned,
					Pinned{Key: c, Mode: Indirect, Via: rk, Path: path})
				toCheck.Remove(c)
			}

			err := checkChildren(path, rk, c)
			if err != nil {
				return err
			}

			if toCheck.Len() == 0 {
				return nil
			}
		}
		return nil
	}

	for path, rk := range recursiveMap {
		if toCheck.Has(rk) {
			pinned = append(pinned,
				Pinned{Key: rk, Mode: Recursive, Path: path})
			toCheck.Remove(rk)
		}

		err := checkChildren(path, rk, rk)
		if err != nil {
			return nil, err
		}
		if toCheck.Len() == 0 {
			break
		}
	}

	// Anything left in toCheck is not pinned
	for _, k := range toCheck.Keys() {
		pinned = append(pinned, Pinned{Key: k, Mode: NotPinned})
	}

	return pinned, nil
}

func cidSetWithValues(cids []cid.Cid) *cid.Set {
	out := cid.NewSet()
	for _, c := range cids {
		out.Add(c)
	}
	return out
}

// PrefixedPins returns a map containing all pins of given kind under given prefix
func (p *pinner) PrefixedPins(prefix string, recursive bool) (map[string]cid.Cid, error) {
	result, err := p.dstore.Query(dsq.Query{Prefix: pathToDSKey(prefix, recursive)})

	if err != nil {
		return nil, err
	}

	pinMap := make(map[string]cid.Cid)

	for entry := range result.Next() {
		c, err := cid.Cast(entry.Value)
		if err != nil {
			return nil, err
		}

		pinMap[entry.Key[len(pinKeyPrefix(recursive)):]] = c
	}
	return pinMap, nil
}

// Update updates a pin from one cid to another
// this is more efficient than simply pinning the new one and unpinning the
// old one
func (p *pinner) Update(ctx context.Context, path string, to cid.Cid) error {
	c, err := p.GetPin(path, true)
	if err != nil {
 		return err
 	}
 	
 	err = dagutils.DiffEnumerate(ctx, p.dserv, *c, to)
	if err != nil {
 		return err
 	}
 	
	err = p.Unpin(path, true)
 	if err != nil {
 		return err
 	}
 	
 	return p.AddPin(path, to, true)
}

// hasChild recursively looks for a Cid among the children of a root Cid.
// The visit function can be used to shortcut already-visited branches.
func hasChild(ctx context.Context, ng ipld.NodeGetter, root cid.Cid, child cid.Cid, visit func(cid.Cid) bool) (bool, error) {
	links, err := ipld.GetLinks(ctx, ng, root)
	if err != nil {
		return false, err
	}
	for _, lnk := range links {
		c := lnk.Cid
		if lnk.Cid.Equals(child) {
			return true, nil
		}
		if visit(c) {
			has, err := hasChild(ctx, ng, c, child, visit)
			if err != nil {
				return false, err
			}

			if has {
				return has, nil
			}
		}
	}
	return false, nil
}

func (p *pinner) PinnedCids(recursive bool) ([]cid.Cid, error) {
	pinMap, err := p.PrefixedPins("", recursive)
	if err != nil {
		return nil, err
	}
	var cids []cid.Cid
	for _, v := range pinMap {
		cids = append(cids, v)
	}
	return cids, nil
}

func (p *pinner) UnpinCid(cid cid.Cid, recursive bool) error {
	path, err := p.IsPinPresent(cid, recursive)
	if err != nil {
		return err
	}
	return p.Unpin(path, recursive)
}

type syncDAGService interface {
	ipld.DAGService
	Sync() error
}
