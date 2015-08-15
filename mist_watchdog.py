from __future__ import with_statement
import mist
import os
import collections
from watchdog.utils.dirsnapshot import DirectorySnapshotDiff
from watchdog.observers.polling import PollingEmitter
from watchdog.observers.api import (
    BaseObserver,
    DEFAULT_OBSERVER_TIMEOUT,
)

from watchdog.events import (
    DirMovedEvent,
    DirDeletedEvent,
    DirCreatedEvent,
    DirModifiedEvent,
    FileMovedEvent,
    FileDeletedEvent,
    FileCreatedEvent,
    FileModifiedEvent
)
from watchdog.events import PatternMatchingEventHandler


class MistWatchdogPollingEmitter(PollingEmitter):
    MODIFYING_DELAY_COUNT = 5

    def __init__(self, *args, **kwargs):
        PollingEmitter.__init__(self, *args, **kwargs)
        self._modifying_files = collections.defaultdict(dict)

    def queue_events(self, timeout):
        # We don't want to hit the disk continuously.
        # timeout behaves like an interval for polling emitters.
        if self.stopped_event.wait(timeout):
            return

        with self._lock:
            if not self.should_keep_running():
                return

            # Get event diff between fresh snapshot and previous snapshot.
            # Update snapshot.
            new_snapshot = self._take_snapshot()
            events = DirectorySnapshotDiff(self._snapshot, new_snapshot)
            self._snapshot = new_snapshot

            # Files.
            for src_path in events.files_deleted:
                self._modifying_files[None][src_path] = MistWatchdogPollingEmitter.MODIFYING_DELAY_COUNT
                self.queue_event(FileDeletedEvent(src_path))

            for src_path in events.files_modified:
                self._modifying_files[src_path][src_path] = MistWatchdogPollingEmitter.MODIFYING_DELAY_COUNT

            for src_path in events.files_created:
                self._modifying_files[src_path][None] = MistWatchdogPollingEmitter.MODIFYING_DELAY_COUNT
                self.queue_event(FileCreatedEvent(src_path))

            for src_path, dest_path in events.files_moved:
                self._modifying_files[dest_path] = self._modifying_files[src_path]
                del self._modifying_files[src_path]
                for modifying_src_path in self._modifying_files[dest_path]:
                    self._modifying_files[dest_path][modifying_src_path] = MistWatchdogPollingEmitter.MODIFYING_DELAY_COUNT
                self.queue_event(FileMovedEvent(src_path, dest_path))

            deletion_list = []

            for modifying_dest_path in self._modifying_files:
                for modifying_src_path in self._modifying_files[modifying_dest_path]:
                    self._modifying_files[modifying_dest_path][modifying_src_path] -= 1
                    if self._modifying_files[modifying_dest_path][modifying_src_path] == 0:
                        if modifying_dest_path is not None and modifying_src_path is not None:
                            self.queue_event(FileDeletedEvent(modifying_src_path))
                            self.queue_event(FileCreatedEvent(modifying_dest_path))
                        elif modifying_dest_path is not None:
                            self.queue_event(FileCreatedEvent(modifying_dest_path))
                        elif modifying_src_path is not None:
                            self.queue_event(FileDeletedEvent(modifying_src_path))
                        deletion_list.append((modifying_dest_path, modifying_src_path))

            for (dest_path, src_path) in deletion_list:
                del self._modifying_files[dest_path][src_path]

            # Directories.
            for src_path in events.dirs_deleted:
                self.queue_event(DirDeletedEvent(src_path))
            for src_path in events.dirs_modified:
                self.queue_event(DirModifiedEvent(src_path))
            for src_path in events.dirs_created:
                self.queue_event(DirCreatedEvent(src_path))
            for src_path, dest_path in events.dirs_moved:
                self.queue_event(DirMovedEvent(src_path, dest_path))


class MistWatchdogObserver(BaseObserver):
    def __init__(self, timeout=DEFAULT_OBSERVER_TIMEOUT):
        BaseObserver.__init__(self, emitter_class=MistWatchdogPollingEmitter, timeout=timeout)


class MistWatchdogEventHandler(PatternMatchingEventHandler):
    """Mist hander which handles all the events captured."""

    IGNORED_PATHS = [
        ".*/.DS_Store",
    ]

    IGNORED_MIST_ROOT_PATHS = [
        (mist.MistFile.STORAGE_FOLDER_PATH, "folder"),
        (mist.Mist.DEFAULT_INDEX_FILENAME, "file"),
    ]

    IGNORED_PATTERNS = [
        "*/.DS_Store",

    ]

    def __init__(self, mist_parent):
        super(MistWatchdogEventHandler, self).__init__(ignore_patterns=MistWatchdogEventHandler.IGNORED_PATTERNS)
        self.mist_parent = mist_parent

    def dispatch(self, event):
        for (path, path_type) in MistWatchdogEventHandler.IGNORED_MIST_ROOT_PATHS:
            full_path = os.path.join(self.mist_parent.root_path, path)
            if path_type == "folder":
                if os.path.commonprefix([event.src_path, full_path]) == full_path:
                    return
            elif path_type == "file":
                if event.src_path == full_path:
                    return

        super(MistWatchdogEventHandler, self).dispatch(event)

    def on_moved(self, event):
        super(MistWatchdogEventHandler, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        print "Moved %s: from %s to %s" % (what, event.src_path, event.dest_path)

        if not event.is_directory:
            self.mist_parent.DeleteFile(event.src_path)
            self.mist_parent.AddFile(event.dest_path)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()

    def on_created(self, event):
        super(MistWatchdogEventHandler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        print "Created %s: %s" % (what, event.src_path)

        if not event.is_directory:
            self.mist_parent.AddFile(event.src_path, False)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()

    def on_deleted(self, event):
        super(MistWatchdogEventHandler, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        print "Deleted %s: %s" % (what, event.src_path)

        if not event.is_directory:
            self.mist_parent.DeleteFile(event.src_path)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()

    def on_modified(self, event):
        super(MistWatchdogEventHandler, self).on_modified(event)

        what = 'directory' if event.is_directory else 'file'
        print "Modified %s: %s" % (what, event.src_path)

        if not event.is_directory:
            self.mist_parent.ModifyFile(event.src_path)

        print "%s:" % self.mist_parent.root_path, self.mist_parent.List()
