#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    // Argument list too long: Too many or too large arguments provided to a program.
    // WASI: ERRNO_2BIG
    ArgumentListTooLong = 1,

    // Permission denied: Insufficient privileges to perform the operation.
    // WASI: ERRNO_ACCES
    PermissionDenied = 2,

    // Address in use: Network address or port already in use.
    // WASI: ERRNO_ADDRINUSE
    AddressInUse = 3,

    // Address not available: The requested address is not available on this system.
    // WASI: ERRNO_ADDRNOTAVAIL
    AddressNotAvailable = 4,

    // Address family not supported: Unsupported address family (e.g., IPv6 on an IPv4-only system).
    // WASI: ERRNO_AFNOSUPPORT
    AddressFamilyNotSupported = 5,

    // Resource unavailable or operation would block: Non-blocking operation cannot proceed at this time.
    // WASI: ERRNO_AGAIN
    ResourceUnavailableOrOperationWouldBlock = 6,

    // Connection already in progress: An operation is already attempting to establish a connection.
    // WASI: ERRNO_ALREADY
    ConnectionAlreadyInProgress = 7,

    // Bad file descriptor: File descriptor is invalid or closed.
    // WASI: ERRNO_BADF
    BadFileDescriptor = 8,

    // Bad message: The message is malformed or corrupted.
    // WASI: ERRNO_BADMSG
    BadMessage = 9,

    // Device or resource busy: The resource is currently in use and cannot be accessed.
    // WASI: ERRNO_BUSY
    DeviceOrResourceBusy = 10,

    // Operation canceled: The operation was canceled before completion.
    // WASI: ERRNO_CANCELED
    OperationCanceled = 11,

    // No child processes: No child processes to wait for in a wait operation.
    // WASI: ERRNO_CHILD
    NoChildProcesses = 12,

    // Connection aborted: The connection was aborted by the host or peer.
    // WASI: ERRNO_CONNABORTED
    ConnectionAborted = 13,

    // Connection refused: The connection attempt was rejected by the remote host.
    // WASI: ERRNO_CONNREFUSED
    ConnectionRefused = 14,

    // Connection reset: The connection was forcibly closed by the peer.
    // WASI: ERRNO_CONNRESET
    ConnectionReset = 15,

    // Resource deadlock would occur: A deadlock was detected and the operation was aborted.
    // WASI: ERRNO_DEADLK
    ResourceDeadlockWouldOccur = 16,

    // Destination address required: No destination address was provided for a network operation.
    // WASI: ERRNO_DESTADDRREQ
    DestinationAddressRequired = 17,

    // Mathematics argument out of domain of function: Invalid input for a mathematical operation.
    // WASI: ERRNO_DOM
    MathematicsArgumentOutOfDomainOfFunction = 18,

    // Reserved error code (not used).
    // WASI: ERRNO_DQUOT
    Reserved19 = 19,

    // File exists: The file or directory already exists.
    // WASI: ERRNO_EXIST
    FileExists = 20,

    // Bad address: A memory address is invalid or inaccessible.
    // WASI: ERRNO_FAULT
    BadAddress = 21,

    // File too large: File size exceeds the system or application limits.
    // WASI: ERRNO_FBIG
    FileTooLarge = 22,

    // Host is unreachable: The remote host cannot be reached.
    // WASI: ERRNO_HOSTUNREACH
    HostIsUnreachable = 23,

    // Identifier removed: The requested identifier has been removed.
    // WASI: ERRNO_IDRM
    IdentifierRemoved = 24,

    // Illegal byte sequence: An invalid or incomplete byte sequence was encountered.
    // WASI: ERRNO_ILSEQ
    IllegalByteSequence = 25,

    // Operation in progress: A long-running operation is still ongoing.
    // WASI: ERRNO_INPROGRESS
    OperationInProgress = 26,

    // Interrupted function: The operation was interrupted by a signal or event.
    // WASI: ERRNO_INTR
    InterruptedFunction = 27,

    // Invalid argument: An argument passed to the function is invalid.
    // WASI: ERRNO_INVAL
    InvalidArgument = 28,

    // I/O error: A low-level input/output operation failed.
    // WASI: ERRNO_IO
    IOError = 29,

    // Socket is connected: The socket is already connected to a remote endpoint.
    // WASI: ERRNO_ISCONN
    SocketIsConnected = 30,

    // Is a directory: The operation is not valid on a directory.
    // WASI: ERRNO_ISDIR
    IsDirectory = 31,

    // Too many levels of symbolic links: A symbolic link loop was detected.
    // WASI: ERRNO_LOOP
    TooManyLevelsOfSymbolicLinks = 32,

    // File descriptor value too large: File descriptor number exceeds the allowed range.
    // WASI: ERRNO_MFILE
    FileDescriptorValueTooLarge = 33,

    // Too many links: The maximum number of hard links has been reached.
    // WASI: ERRNO_MLINK
    TooManyLinks = 34,

    // Message too large: A network message exceeds the size limit.
    // WASI: ERRNO_MSGSIZE
    MessageTooLarge = 35,

    // Reserved error code (not used).
    // WASI: ERRNO_MULTIHOP
    Reserved36 = 36,

    // Filename too long: A file or directory name exceeds the allowed length.
    // WASI: ERRNO_NAMETOOLONG
    FilenameTooLong = 37,

    // Network is down: The network is currently unavailable.
    // WASI: ERRNO_NETDOWN
    NetworkIsDown = 38,

    // Connection aborted by network: A network issue caused the connection to be aborted.
    // WASI: ERRNO_NETRESET
    ConnectionAbortedByNetwork = 39,

    // Network unreachable: The network cannot be accessed.
    // WASI: ERRNO_NETUNREACH
    NetworkUnreachable = 40,

    // Too many files open in system: System-wide file descriptor limit exceeded.
    // WASI: ERRNO_NFILE
    TooManyFilesOpenInSystem = 41,

    // No buffer space available: Insufficient buffer space for the operation.
    // WASI: ERRNO_NOBUFS
    NoBufferSpaceAvailable = 42,

    // No such device: The requested device does not exist.
    // WASI: ERRNO_NODEV
    NoSuchDevice = 43,

    // No such file or directory: The specified file or directory was not found.
    // WASI: ERRNO_NOENT
    NoSuchFileOrDirectory = 44,

    // Executable file format error: The file is not a valid executable format.
    // WASI: ERRNO_NOEXEC
    ExecutableFileFormatError = 45,

    // No locks available: The system has no more locks available for use.
    // WASI: ERRNO_NOLCK
    NoLocksAvailable = 46,

    // Reserved error code (not used).
    // WASI: ERRNO_NOLINK
    Reserved47 = 47,

    // Not enough space: Insufficient storage space for the operation.
    // WASI: ERRNO_NOMEM
    NotEnoughSpace = 48,

    // No message of the desired type: A requested message type is unavailable.
    // WASI: ERRNO_NOMSG
    NoMessageOfTheDesiredType = 49,

    // Protocol not available: The requested protocol is unavailable.
    // WASI: ERRNO_NOPROTOOPT
    ProtocolNotAvailable = 50,

    // No space left on device: The storage device is full.
    // WASI: ERRNO_NOSPC
    NoSpaceLeftOnDevice = 51,

    // Function not supported: The operation is not supported by the system or device.
    // WASI: ERRNO_NOSYS
    FunctionNotSupported = 52,

    // Socket not connected: The socket is not connected to a remote endpoint.
    // WASI: ERRNO_NOTCONN
    SocketNotConnected = 53,

    // Not a directory or symbolic link: The target is not a directory or valid symbolic link.
    // WASI: ERRNO_NOTDIR
    NotADirectoryOrSymbolicLink = 54,

    // Directory not empty: Cannot delete a directory that is not empty.
    // WASI: ERRNO_NOTEMPTY
    DirectoryNotEmpty = 55,

    // State not recoverable: A persistent state could not be restored.
    // WASI: ERRNO_NOTRECOVERABLE
    StateNotRecoverable = 56,

    // Not a socket: The file descriptor is not a socket.
    // WASI: ERRNO_NOTSOCK
    NotASocket = 57,

    // Not supported or operation not supported on socket: Unsupported operation on the socket.
    // WASI: ERRNO_NOTSUP
    NotSupportedOrOperationNotSupportedOnSocket = 58,

    // Inappropriate I/O control operation: The ioctl operation is inappropriate for the device.
    // WASI: ERRNO_NOTTY
    InappropriateIOControlOperation = 59,

    // No such device or address: The device or address does not exist.
    // WASI: ERRNO_NXIO
    NoSuchDeviceOrAddress = 60,

    // Value too large to be stored in data type: A value exceeds the data type's range.
    // WASI: ERRNO_OVERFLOW
    ValueTooLargeToBeStoredInDataType = 61,

    // Previous owner died: The previous owner of a mutex or resource has died.
    // WASI: ERRNO_OWNERDEAD
    PreviousOwnerDied = 62,

    // Operation not permitted: The operation is prohibited by the system or security policy.
    // WASI: ERRNO_PERM
    OperationNotPermitted = 63,

    // Broken pipe: A connection was closed while writing data.
    // WASI: ERRNO_PIPE
    BrokenPipe = 64,

    // Protocol error: A protocol error occurred during communication.
    // WASI: ERRNO_PROTO
    ProtocolError = 65,

    // Protocol not supported: The protocol is not supported by the system.
    // WASI: ERRNO_PROTONOSUPPORT
    ProtocolNotSupported = 66,

    // Protocol wrong type for socket: The protocol does not match the socket type.
    // WASI: ERRNO_PROTOTYPE
    ProtocolWrongTypeForSocket = 67,

    // Result too large: The result of an operation exceeds the allowable limit.
    // WASI: ERRNO_RANGE
    ResultTooLarge = 68,

    // Read-only file system: Attempted to modify a read-only file system.
    // WASI: ERRNO_ROFS
    ReadOnlyFileSystem = 69,

    // Invalid seek: An invalid file seek operation was attempted.
    // WASI: ERRNO_SPIPE
    InvalidSeek = 70,

    // No such process: The specified process does not exist.
    // WASI: ERRNO_SRCH
    NoSuchProcess = 71,

    // Reserved error code (not used).
    // WASI: ERRNO_STALE
    Reserved72 = 72,

    // Connection timed out: The connection attempt timed out.
    // WASI: ERRNO_TIMEDOUT
    ConnectionTimedOut = 73,

    // Text file busy: An attempt to modify an in-use text file.
    // WASI: ERRNO_TXTBSY
    TextFileBusy = 74,

    // Cross-device link: An operation attempted to link across different devices.
    // WASI: ERRNO_XDEV
    CrossDeviceLink = 75,

    // Extension capabilities insufficient: The required extension capabilities are missing.
    // WASI: ERRNO_NOTCAPABLE
    ExtensionCapabilitiesInsufficient = 76,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for Error {}
