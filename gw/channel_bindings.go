package gateway

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/iodesystems/gwag/v2/gw/ir"
)

// channelBindingEntry is one aggregated binding with its owning slot
// coordinate. Used by the gateway-wide index for publish-time
// payload_type stamping and admin enumeration.
type channelBindingEntry struct {
	pattern     string
	messageFQN  string
	namespace   string
	version     string
	messageDesc protoreflect.MessageDescriptor
}

// channelBindingIndex is the gateway-wide aggregation of all slot
// channel bindings. Rebuilt on every assembleLocked and atomic-
// swapped so psPub can look up payload_type without holding g.mu.
type channelBindingIndex struct {
	entries []channelBindingEntry
}

// lookupPayloadType returns the MessageFQN for the first binding
// pattern that matches the given channel subject, or "" if no match.
// Uses NATS-style pattern matching (same grammar as WithChannelAuth).
func (idx *channelBindingIndex) lookupPayloadType(channel string) string {
	if idx == nil {
		return ""
	}
	for _, e := range idx.entries {
		if subjectMatchesPattern(e.pattern, channel) {
			return e.messageFQN
		}
	}
	return ""
}

// lookupBinding returns the full binding entry for the first pattern
// that matches the channel, or nil. Used by the enforcement path to
// access the message descriptor for payload validation.
func (idx *channelBindingIndex) lookupBinding(channel string) *channelBindingEntry {
	if idx == nil {
		return nil
	}
	for i := range idx.entries {
		if subjectMatchesPattern(idx.entries[i].pattern, channel) {
			return &idx.entries[i]
		}
	}
	return nil
}

// validatePayload attempts to parse the payload JSON as the bound
// proto message. Returns nil if the payload is valid, or a Reject
// error with CodeInvalidArgument if parsing fails.
func (idx *channelBindingIndex) validatePayload(channel, payload string) error {
	binding := idx.lookupBinding(channel)
	if binding == nil || binding.messageDesc == nil {
		return nil
	}
	msg := dynamicpb.NewMessage(binding.messageDesc)
	if err := protojson.Unmarshal([]byte(payload), msg); err != nil {
		return Reject(CodeInvalidArgument, fmt.Sprintf("ps.pub: payload for channel %q does not match bound type %s: %v", channel, binding.messageFQN, err))
	}
	return nil
}

// rebuildChannelBindingIndexLocked aggregates channelBindings from
// every slot into a fresh index and atomic-swaps it. Also resolves
// message descriptors from registered proto slots for payload
// validation. Caller holds g.mu.
func (g *Gateway) rebuildChannelBindingIndexLocked() {
	// Build a message FQN → descriptor lookup from all proto slots.
	msgDescs := g.resolveMessageDescriptorsLocked()
	var entries []channelBindingEntry
	for _, s := range g.slots {
		for _, b := range s.channelBindings {
			entry := channelBindingEntry{
				pattern:    b.Pattern,
				messageFQN: b.MessageFQN,
				namespace:  s.key.namespace,
				version:    s.key.version,
			}
			if desc, ok := msgDescs[b.MessageFQN]; ok {
				entry.messageDesc = desc
			}
			entries = append(entries, entry)
		}
	}
	idx := &channelBindingIndex{entries: entries}
	g.channelBindingIndex.Store(idx)
}

// resolveMessageDescriptorsLocked walks all proto and internal-proto
// slots and returns a map from message FQN to MessageDescriptor.
// Used by rebuildChannelBindingIndexLocked to resolve bound types
// for payload validation. Caller holds g.mu.
func (g *Gateway) resolveMessageDescriptorsLocked() map[string]protoreflect.MessageDescriptor {
	out := make(map[string]protoreflect.MessageDescriptor)
	for _, s := range g.slots {
		var fd protoreflect.FileDescriptor
		switch {
		case s.proto != nil:
			fd = s.proto.file
		case s.internalProto != nil:
			fd = s.internalProto.file
		default:
			continue
		}
		walkMessagesForDescriptors(fd, out)
	}
	return out
}

func walkMessagesForDescriptors(fd protoreflect.FileDescriptor, out map[string]protoreflect.MessageDescriptor) {
	var walk func(protoreflect.MessageDescriptors)
	walk = func(ms protoreflect.MessageDescriptors) {
		for i := 0; i < ms.Len(); i++ {
			md := ms.Get(i)
			out[string(md.FullName())] = md
			walk(md.Messages())
		}
	}
	walk(fd.Messages())
}

// channelBindingIndexSnapshot returns a copy of the current binding
// index entries for admin enumeration. Does NOT hold g.mu — the
// index is atomic-loaded.
func (g *Gateway) channelBindingIndexSnapshot() []channelBindingEntry {
	idx := g.channelBindingIndex.Load()
	if idx == nil {
		return nil
	}
	out := make([]channelBindingEntry, len(idx.entries))
	copy(out, idx.entries)
	return out
}

// channelBindingExtensionFullName is the proto full name of the
// `(gwag.ps.binding)` extension defined in
// `gw/proto/ps/v1/options.proto`. We compare by name rather than by
// the generated `psv1.E_Binding` extension type because protocompile
// resolves options against its own descriptor pool — the option's
// value comes back as `*dynamicpb.Message`, not the concrete
// `*psv1.ChannelBinding`, so `proto.GetExtension` would panic on the
// type assertion. The protoreflect Range over MessageOptions sees the
// same field regardless of which pool the descriptor came from.
const channelBindingExtensionFullName = "gwag.ps.v1.binding"

// channelBindingPatternFieldName is the field on `ChannelBinding`
// whose value we extract.
const channelBindingPatternFieldName = "pattern"

// extractChannelBindings walks every message reachable from `fd`
// (top-level + nested, but not imported files — bindings travel with
// their declaring file's slot) and returns the `(gwag.ps.binding)`
// extensions stamped on them. Pattern is the literal NATS-style
// pattern the option declared; MessageFQN is the message's proto
// full name.
//
// Messages without the option, and proto files that don't import
// `gw/proto/ps/v1/options.proto`, yield no entries. Idempotent across
// rebakes — the IR slice is rebuilt from scratch each call.
func extractChannelBindings(fd protoreflect.FileDescriptor) []ir.ChannelBinding {
	if fd == nil {
		return nil
	}
	var out []ir.ChannelBinding
	walkMessagesForBindings(fd.Messages(), &out)
	return out
}

func walkMessagesForBindings(ms protoreflect.MessageDescriptors, out *[]ir.ChannelBinding) {
	for i := 0; i < ms.Len(); i++ {
		md := ms.Get(i)
		if pattern := bindingPatternOnMessage(md); pattern != "" {
			*out = append(*out, ir.ChannelBinding{
				Pattern:    pattern,
				MessageFQN: string(md.FullName()),
			})
		}
		walkMessagesForBindings(md.Messages(), out)
	}
}

// bindingPatternOnMessage returns the pattern string on the
// `(gwag.ps.binding)` extension if it's set on md's MessageOptions,
// or "" if no binding is declared. Works against both dynamicpb
// option values (protocompile) and the generated concrete type
// (boot-time Go-registered extension).
func bindingPatternOnMessage(md protoreflect.MessageDescriptor) string {
	opts := md.Options()
	if opts == nil {
		return ""
	}
	var pattern string
	opts.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if !fd.IsExtension() || string(fd.FullName()) != channelBindingExtensionFullName {
			return true
		}
		bindingMsg := v.Message()
		patternFD := bindingMsg.Descriptor().Fields().ByName(channelBindingPatternFieldName)
		if patternFD == nil {
			return true
		}
		pattern = bindingMsg.Get(patternFD).String()
		return false
	})
	return pattern
}

// WithChannelBinding registers a channel→payload-type pairing at the
// gateway level, for non-proto adopters and gateway-shipped defaults.
// Pattern uses NATS-style wildcards (same grammar as WithChannelAuth).
// MessageFQN is the proto fully-qualified message name
// ("<package>.<Name>").
//
// Multiple calls compose; declaration order is preserved. Bindings are
// applied to the gateway-internal ps slot during New() and route
// through the same tier/uniqueness policy as proto-declarative
// bindings — cross-slot pattern conflicts are rejected, and vN slots
// lock by (pattern, payload_fqn).
//
// Unlike the proto-declarative path (which extracts bindings from the
// `(gwag.ps.binding)` message option at slot bake time), this API
// accepts the FQN string directly, avoiding the need for the caller
// to have the proto message type available at gateway construction.
//
// Stability: stable
func WithChannelBinding(pattern, messageFQN string) Option {
	return func(cfg *config) {
		cfg.channelBindings = append(cfg.channelBindings, ir.ChannelBinding{
			Pattern:    pattern,
			MessageFQN: messageFQN,
		})
	}
}

// applyRuntimeBindingsLocked merges runtime-declared bindings
// (WithChannelBinding) into the gateway-internal ps slot. Runs the
// cross-slot uniqueness check before mutating. Caller does NOT hold
// g.mu — acquires it internally.
func (g *Gateway) applyRuntimeBindingsLocked(bindings []ir.ChannelBinding) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	psKey := poolKey{namespace: psNamespace, version: psVersion}
	psSlot, ok := g.slots[psKey]
	if !ok {
		return fmt.Errorf("channel binding: ps slot %s/%s not installed (requires cluster)", psNamespace, psVersion)
	}

	// Check cross-slot uniqueness before mutating.
	if err := g.checkCrossSlotBindingsLocked(psKey, bindings); err != nil {
		return err
	}

	psSlot.channelBindings = append(psSlot.channelBindings, bindings...)
	g.bakeSlotIRLocked(psSlot)
	return g.assembleLocked()
}

// WithChannelBindingEnforce enables shape strictness for ps.pub: when
// a channel has a binding, the payload is parsed as the bound proto
// message and rejected with InvalidArgument if it doesn't match.
// Default false (permissive). See WithStrictPayloadTypes for coverage
// strictness. Both can be enabled together.
//
// Stability: stable
func WithChannelBindingEnforce() Option {
	return func(cfg *config) { cfg.psBindingEnforce = true }
}

// WithStrictPayloadTypes enables coverage strictness for ps.pub:
// rejects publishes to channels that match no WithChannelBinding
// pattern (where payload_type would otherwise be blank). Default
// false (permissive) so the open tier stays usable for ad-hoc / dev
// channels. See WithChannelBindingEnforce for shape strictness. Both
// can be enabled together.
//
// Stability: stable
func WithStrictPayloadTypes() Option {
	return func(cfg *config) { cfg.psStrictPayloadTypes = true }
}
