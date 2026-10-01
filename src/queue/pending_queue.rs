use crate::chunk::chunk_payload_data::ChunkPayloadData;

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use std::time::Instant;

/// pendingBaseQueue
pub(crate) type PendingBaseQueue = VecDeque<ChunkPayloadData>;

/// pendingQueue
#[derive(Debug, Default)]
pub(crate) struct PendingQueue {
    unordered_queue: PendingBaseQueue,
    ordered_queue: PendingBaseQueue,
    queue_len: usize,
    n_bytes: usize,
    /// Queued chunks that carry a timed-reliability lifetime.
    n_timed: usize,
    selected: bool,
    unordered_is_selected: bool,
}

impl PendingQueue {
    pub(crate) fn new() -> Self {
        PendingQueue::default()
    }

    pub(crate) fn push(&mut self, c: ChunkPayloadData) {
        self.n_bytes += c.user_data.len();
        self.n_timed += usize::from(c.lifetime.is_some());
        if c.unordered {
            self.unordered_queue.push_back(c);
        } else {
            self.ordered_queue.push_back(c);
        }
        self.queue_len += 1;
    }

    pub(crate) fn peek(&self) -> Option<&ChunkPayloadData> {
        if self.selected {
            if self.unordered_is_selected {
                return self.unordered_queue.front();
            } else {
                return self.ordered_queue.front();
            }
        }

        let c = self.unordered_queue.front();

        if c.is_some() {
            return c;
        }

        self.ordered_queue.front()
    }

    pub(crate) fn pop(
        &mut self,
        beginning_fragment: bool,
        unordered: bool,
    ) -> Option<ChunkPayloadData> {
        let popped = if self.selected {
            let popped = if self.unordered_is_selected {
                self.unordered_queue.pop_front()
            } else {
                self.ordered_queue.pop_front()
            };
            if let Some(p) = &popped {
                if p.ending_fragment {
                    self.selected = false;
                }
            }
            popped
        } else {
            if !beginning_fragment {
                return None;
            }
            if unordered {
                let popped = { self.unordered_queue.pop_front() };
                if let Some(p) = &popped {
                    if !p.ending_fragment {
                        self.selected = true;
                        self.unordered_is_selected = true;
                    }
                }
                popped
            } else {
                let popped = { self.ordered_queue.pop_front() };
                if let Some(p) = &popped {
                    if !p.ending_fragment {
                        self.selected = true;
                        self.unordered_is_selected = false;
                    }
                }
                popped
            }
        };

        if let Some(p) = &popped {
            self.n_bytes -= p.user_data.len();
            self.n_timed -= usize::from(p.lifetime.is_some());
            self.queue_len -= 1;
        }

        popped
    }

    /// Starts the lifetime of newly queued timed messages and removes every
    /// message whose lifetime ran out before any of its fragments received a
    /// TSN (RFC 3758 §4.1 and TR3). A partially sent message cannot be removed
    /// here: it is marked abandoned so its unsent tail is discarded and
    /// FORWARD-TSN covers the sent prefix.
    ///
    /// An ordered message never sent leaves no gap in its stream: messages
    /// queued behind it take over its stream sequence number, as if it had
    /// never been queued. The removed chunks are returned so the caller can
    /// release their buffer credit and stream sequence numbers.
    pub(crate) fn remove_expired(&mut self, now: Instant) -> Vec<ChunkPayloadData> {
        let mut removed = Vec::new();
        if self.n_timed == 0 {
            return removed;
        }
        for unordered in [true, false] {
            let mut sending = self.selected && self.unordered_is_selected == unordered;
            let mut expired = false;
            // (stream, ordered messages removed so far) for the SSN shift.
            let mut shifts: Vec<(u16, u16)> = Vec::new();
            let queue = if unordered {
                &mut self.unordered_queue
            } else {
                &mut self.ordered_queue
            };
            queue.retain_mut(|c| {
                if let Some(lifetime) = c.lifetime {
                    c.expires_at.get_or_insert(now + lifetime);
                }
                if sending {
                    sending = !c.ending_fragment;
                    if c.expired(now) {
                        c.abandon();
                    }
                    return true;
                }
                if c.beginning_fragment {
                    expired = c.expired(now);
                    if expired && !unordered {
                        match shifts.iter_mut().find(|(id, _)| *id == c.stream_identifier) {
                            Some((_, n)) => *n = n.wrapping_add(1),
                            None => shifts.push((c.stream_identifier, 1)),
                        }
                    }
                }
                if expired {
                    removed.push(c.clone());
                    return false;
                }
                if let Some((_, n)) = shifts.iter().find(|(id, _)| *id == c.stream_identifier) {
                    c.stream_sequence_number = c.stream_sequence_number.wrapping_sub(*n);
                }
                true
            });
        }
        for c in &removed {
            self.n_bytes -= c.user_data.len();
            self.n_timed -= usize::from(c.lifetime.is_some());
            self.queue_len -= 1;
        }
        removed
    }

    pub(crate) fn get_num_bytes(&self) -> usize {
        self.n_bytes
    }

    pub(crate) fn len(&self) -> usize {
        self.queue_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn contains_stream(&self, stream_identifier: u16) -> bool {
        self.unordered_queue
            .iter()
            .chain(self.ordered_queue.iter())
            .any(|chunk| chunk.stream_identifier == stream_identifier)
    }
}
