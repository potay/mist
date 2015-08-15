import mist
import os
import re
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class MistWatchdogObserver(Observer):
    pass


class MistWatchdogEventHandler(FileSystemEventHandler):
    """Mist hander which handles all the events captured."""

    IGNORED_PATHS = [
        ".*/.DS_Store",
    ]

    IGNORED_MIST_ROOT_PATHS = [
        (mist.MistFile.STORAGE_FOLDER_PATH, "folder"),
        (mist.Mist.DEFAULT_INDEX_FILENAME, "file"),
    ]

    def __init__(self, mist_parent, *args, **kwargs):
        super(MistWatchdogEventHandler, self).__init__(*args, **kwargs)
        self.mist_parent = mist_parent

    def dispatch(self, event):
        combined = "(" + ")|(".join(MistWatchdogEventHandler.IGNORED_PATHS) + ")"

        for (path, path_type) in MistWatchdogEventHandler.IGNORED_MIST_ROOT_PATHS:
            full_path = os.path.join(self.mist_parent.root_path, path)
            if path_type == "folder":
                if os.path.commonprefix([event.src_path, full_path]) == full_path:
                    return
            elif path_type == "file":
                if event.src_path == full_path:
                    return

        if not re.match(combined, event.src_path):
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
