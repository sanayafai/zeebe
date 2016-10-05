package org.camunda.tngp.list;

import java.util.Iterator;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class CompactListIterator implements Iterator<MutableDirectBuffer>
{
    protected final CompactList values;
    protected final UnsafeBuffer current;
    protected int position;

    public CompactListIterator(final CompactList values)
    {
        this.values = values;
        this.current = new UnsafeBuffer(new byte[values.maxElementDataLength()]);

        reset();
    }

    /**
     * Reset the current position of iterator.
     */
    public void reset()
    {
        position = -1;
    }

    /**
     * Return the current position of the iterator.
     * @return
     */
    public int position()
    {
        return position;
    }

    @Override
    public boolean hasNext()
    {
        return position + 1 < values.size();
    }

    /**
     * Attach a view of the next element to a {@link MutableDirectBuffer} for providing direct access.
     * Always returns the same object, i.e. objects returned by previous {@link #next()} invocations
     * become invalid.
     *
     * @see CompactList#wrap(int, MutableDirectBuffer)
     * @see Iterator#next()
     */
    public MutableDirectBuffer next()
    {
        if (position + 1 >= values.size())
        {
            throw new java.util.NoSuchElementException();
        }

        position++;
        values.wrap(position, current);

        return current;
    }

}
