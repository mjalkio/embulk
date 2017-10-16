package org.embulk.spi.util;

import java.util.List;
import java.util.Map;
import com.google.common.base.Optional;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.spi.Schema;
import org.embulk.spi.Column;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampFormat;
import org.embulk.spi.util.dynamic.SkipColumnSetter;

public class DynamicPageBuilder
        implements AutoCloseable
{
    private final PageBuilder pageBuilder;
    private final Schema schema;
    private final DynamicColumnSetter[] setters;
    private final Map<String, DynamicColumnSetter> columnLookup;

    public static interface BuilderTask
            extends Task
    {
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public DateTimeZone getDefaultTimeZone();

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, ConfigSource> getColumnOptions();

        // required by TimestampFormatter
        @ConfigInject
        @Deprecated
        public ScriptingContainer getJRuby();
    }

    public static interface ColumnOption
            extends Task
    {
        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%6N\"")
        public TimestampFormat getTimestampFormat();

        @Config("timezone")
        @ConfigDefault("null")
        public Optional<DateTimeZone> getTimeZone();
    }

    private DynamicPageBuilder(
            final DynamicColumnSetterFactory factory,
            final BufferAllocator allocator,
            final Schema schema,
            final PageOutput output)
    {
        this.pageBuilder = new PageBuilder(allocator, schema, output);
        this.schema = schema;
        ImmutableList.Builder<DynamicColumnSetter> setters = ImmutableList.builder();
        ImmutableMap.Builder<String, DynamicColumnSetter> lookup = ImmutableMap.builder();
        for (Column c : schema.getColumns()) {
            DynamicColumnSetter setter = factory.newColumnSetter(pageBuilder, c);
            setters.add(setter);
            lookup.put(c.getName(), setter);
        }
        this.setters = setters.build().toArray(new DynamicColumnSetter[0]);
        this.columnLookup = lookup.build();
    }

    @Deprecated  // The static creator method "createWithTimestampMetadataFromBuilderTask" is preferred.
    public DynamicPageBuilder(
            BuilderTask task,
            BufferAllocator allocator,
            Schema schema,
            PageOutput output)
    {
        this(DynamicColumnSetterFactory.createWithTimestampMetadataFromBuilderTask(
                 task, DynamicColumnSetterFactory.nullDefaultValue()),  // TODO configurable default value
             allocator,
             schema,
             output);
    }

    public static DynamicPageBuilder createWithTimestampMetadataFromBuilderTask(
            BuilderTask task,
            BufferAllocator allocator,
            Schema schema,
            PageOutput output)
    {
        // TODO configurable default value
        DynamicColumnSetterFactory factory = DynamicColumnSetterFactory.createWithTimestampMetadataFromBuilderTask(
                task, DynamicColumnSetterFactory.nullDefaultValue());
        return new DynamicPageBuilder(factory, allocator, schema, output);
    }

    public static DynamicPageBuilder createWithTimestampMetadataFromColumn(
            BuilderTask task,
            BufferAllocator allocator,
            Schema schema,
            PageOutput output)
    {
        // TODO configurable default value
        DynamicColumnSetterFactory factory = DynamicColumnSetterFactory.createWithTimestampMetadataFromColumn(
                task, DynamicColumnSetterFactory.nullDefaultValue());
        return new DynamicPageBuilder(factory, allocator, schema, output);
    }

    public List<Column> getColumns()
    {
        return schema.getColumns();
    }

    public DynamicColumnSetter column(Column c)
    {
        return setters[c.getIndex()];
    }

    public DynamicColumnSetter column(int index)
    {
        if (index < 0 || setters.length <= index) {
            throw new DynamicColumnNotFoundException("Column index '"+index+"' is not exist");
        }
        return setters[index];
    }

    public DynamicColumnSetter lookupColumn(String columnName)
    {
        DynamicColumnSetter setter = columnLookup.get(columnName);
        if (setter == null) {
            throw new DynamicColumnNotFoundException("Column '"+columnName+"' is not exist");
        }
        return setter;
    }

    public DynamicColumnSetter columnOrSkip(int index)
    {
        if (index < 0 || setters.length <= index) {
            return SkipColumnSetter.get();
        }
        return setters[index];
    }

    public DynamicColumnSetter columnOrSkip(String columnName)
    {
        DynamicColumnSetter setter = columnLookup.get(columnName);
        if (setter == null) {
            return SkipColumnSetter.get();
        }
        return setter;
    }

    // for jruby
    protected DynamicColumnSetter columnOrNull(int index)
    {
        if (index < 0 || setters.length <= index) {
            return null;
        }
        return setters[index];
    }

    // for jruby
    protected DynamicColumnSetter columnOrNull(String columnName)
    {
        return columnLookup.get(columnName);
    }

    public void addRecord()
    {
        pageBuilder.addRecord();
    }

    public void flush()
    {
        pageBuilder.flush();
    }

    public void finish()
    {
        pageBuilder.finish();
    }

    @Override
    public void close()
    {
        pageBuilder.close();
    }
}
