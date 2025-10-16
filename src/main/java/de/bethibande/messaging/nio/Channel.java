package de.bethibande.messaging.nio;

import de.bethibande.memory.Buffer;
import de.bethibande.memory.impl.CompositeBuffer;
import de.bethibande.memory.impl.DefaultBuffer;
import de.bethibande.memory.impl.ExpandingBuffer;
import de.bethibande.memory.impl.JavaNioBuffer;
import de.bethibande.messaging.concurrent.EventLoop;
import de.bethibande.messaging.concurrent.Selectable;
import de.bethibande.messaging.locking.SpinningLock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Queue;

public class Channel implements Selectable {

    protected static final long BUFFER_REGION_SIZE = 1L << 16;

    protected final SocketChannel socket;
    protected SelectionKey key;

    protected final EventLoop loop;

    protected final Queue<Buffer> writeQueue = new ArrayDeque<>();
    protected final SpinningLock writeLock = new SpinningLock();

    protected final ExpandingBuffer readBuffer;

    public Channel(final SocketChannel socket, final EventLoop loop) throws IOException {
        this.socket = socket;
        this.loop = loop;

        this.readBuffer = new ExpandingBuffer(4, 16, loop.allocator());
    }

    /**
     * Will bind the socket to the event loop selector by registering its selection key.
     * This will also set the SocketChannel to non-blocking mode.
     *
     * @throws IOException If an I/O error occurs.
     */
    protected void bind() throws IOException {
        this.socket.configureBlocking(false);
        this.key = this.socket.register(this.loop.selector(), SelectionKey.OP_READ);
        this.key.attach(this);
    }

    @Override
    public boolean update() {
        if (!this.socket.isOpen()) return false;

        try {
            if (this.key.isWritable()) {
                if (!this.writeQueue.isEmpty()) {
                    return tryWrite();
                } else {
                    final long ticket = this.writeLock.lockSpinning();
                    try {
                        this.key.interestOps(SelectionKey.OP_READ);
                    } finally {
                        this.writeLock.unlock(ticket);
                    }
                    return false;
                }
            }

            if (this.key.isReadable()) {
                return tryRead();
            }
        } catch (final IOException ex) {
            ex.printStackTrace();
            // TODO: Error handling
        }

        return false;
    }

    protected boolean tryWrite() throws IOException {
        final Buffer buffer;
        final boolean nextOperation;

        final long ticket = this.writeLock.lockSpinning();
        try {
            buffer = this.writeQueue.poll();
            nextOperation = !this.writeQueue.isEmpty();
            if (!nextOperation) this.key.interestOps(SelectionKey.OP_READ);
        } finally {
            this.writeLock.unlock(ticket);
        }

        switch (buffer) {
            case null -> {
                return false;
            }
            case CompositeBuffer composite -> {
                final ByteBuffer[] nioBuffers = toNioBuffers(composite, buffer.readPosition(), buffer.readable());
                this.socket.write(nioBuffers);

                buffer.release();
            }
            case DefaultBuffer defaultBuffer -> {
                final ByteBuffer nioBuffer = defaultBuffer.asNioBuffer();
                nioBuffer.position((int) defaultBuffer.readPosition());
                nioBuffer.limit((int) (defaultBuffer.readPosition() + defaultBuffer.readable()));

                this.socket.write(nioBuffer);
                defaultBuffer.release();
            }
            case JavaNioBuffer javaNioBuffer -> {
                final ByteBuffer nioBuffer = javaNioBuffer.unwrap();
                nioBuffer.position((int) javaNioBuffer.readPosition());
                nioBuffer.limit((int) (javaNioBuffer.readPosition() + javaNioBuffer.readable()));

                this.socket.write(nioBuffer);
                javaNioBuffer.release();
            }
            default -> {
                buffer.release();
                throw new IllegalStateException("Unknown buffer type: " + buffer.getClass().getName() + "; must be an instance of either CompositeBuffer or DefaultBuffer");
            }
        }

        return nextOperation;
    }

    protected boolean tryRead() throws IOException {
        while (this.readBuffer.writable() < BUFFER_REGION_SIZE * 4) {
            this.readBuffer.expand();
        }

        final ByteBuffer[] buffers = toNioBuffers(this.readBuffer, this.readBuffer.writePosition(), this.readBuffer.writable());
        final long read = this.socket.read(buffers);
        if (read == 0) return false;
        if (read < 0) {
            close();
            return false;
        }

        System.out.println("Read: " + read + " bytes");

        this.readBuffer.writePosition(this.readBuffer.writePosition() + read);
        // TODO: Process data
        this.readBuffer.readPosition(this.readBuffer.readPosition() + read);

        this.readBuffer.compact(); // Discard all processed buffers from the composite buffer

        return true;
    }

    public void connect(final InetSocketAddress address) throws IOException {
        this.socket.connect(address);
        this.socket.finishConnect();

        this.bind();
    }

    protected ByteBuffer[] toNioBuffers(final CompositeBuffer buffer, final long start, final long length) {
        final int startBufferIndex = (int) (start >> 16);
        final int endBufferIndex = (int) ((start + length) >> 16) - 1;
        final int bufferCount = endBufferIndex - startBufferIndex + 1;

        final ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        long offset = start;
        int index = 0;
        while (offset < start + length) {
            final Buffer buf = buffer.bufferAt(offset);
            buffers[index++] = ((JavaNioBuffer) buf).unwrap();

            offset += Math.min(length - (offset - start), BUFFER_REGION_SIZE);
        }
        buffers[0].position((int) (start & 0xFFFF));

        return buffers;
    }

    public void write(final Buffer buffer) {
        final long ticket = this.writeLock.lockSpinning();
        try {
            this.writeQueue.offer(buffer);
            this.key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ); // Add write interest, otherwise the queue will never be flushed
        } finally {
            this.writeLock.unlock(ticket);
        }
    }

    public void close() throws IOException {
        this.key.cancel();
        this.socket.close(); // TODO: Find a better solution for this
    }

}
