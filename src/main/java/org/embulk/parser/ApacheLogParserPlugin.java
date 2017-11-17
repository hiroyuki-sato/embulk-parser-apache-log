package org.embulk.parser;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

import org.embulk.spi.Exec;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.util.LineDecoder;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.ColumnConfig;
import java.util.ArrayList;

//import static org.embulk.spi.type.Types.BOOLEAN;
//import static org.embulk.spi.type.Types.DOUBLE;
//import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Throwables;

public class ApacheLogParserPlugin
        implements ParserPlugin
{
    public enum LogFormat
    {
         combined("combined"),
         common("common");
         private final String string;

         private LogFormat(String string)
         {
             this.string = string;
         }
         public String getString()
         {
             return string;
         }
    }
    public interface PluginTask
            extends Task, LineDecoder.DecoderTask, TimestampParser.Task
    {

        @Config("format")
        @ConfigDefault("\"combined\"")
        public LogFormat getFormat();

    }

    @Override
    public void transaction(ConfigSource config, ParserPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        ArrayList<ColumnConfig> columns = new ArrayList<ColumnConfig>();
        final LogFormat format = task.getFormat();

        columns.add(new ColumnConfig("remote_host",STRING ,config));
        columns.add(new ColumnConfig("identity_check",STRING ,config));
        columns.add(new ColumnConfig("user",STRING ,config));
        columns.add(new ColumnConfig("datetime",TIMESTAMP,config));
        columns.add(new ColumnConfig("method",STRING ,config));
        columns.add(new ColumnConfig("path",STRING ,config));
        columns.add(new ColumnConfig("protocol",STRING ,config));
        columns.add(new ColumnConfig("status",STRING ,config));
        columns.add(new ColumnConfig("size",STRING ,config));

        // combined
        if( format == LogFormat.combined ){
          columns.add(new ColumnConfig("referer",STRING ,config));
          columns.add(new ColumnConfig("user_agent",STRING ,config));
        }

        Schema schema = new SchemaConfig(columns).toSchema();
        control.run(task.dump(), schema);
    }

    private static interface ParserIntlTask extends Task, TimestampParser.Task {}
    private static interface ParserIntlColumnOption extends Task, TimestampParser.TimestampColumnOption {}

    @Override
    public void run(TaskSource taskSource, Schema schema,
            FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        LineDecoder lineDecoder = new LineDecoder(input,task);
        PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output);
        String line = null;
        final LogFormat format = task.getFormat();

        Pattern accessLogPattern = Pattern.compile(getAccessLogRegex(format),
                                                     Pattern.CASE_INSENSITIVE
                                                   | Pattern.DOTALL);
        Matcher accessLogEntryMatcher;
        // TODO: Switch to a newer TimestampParser constructor after a reasonable interval.
        // Traditional constructor is used here for compatibility.
        final ConfigSource configSource = Exec.newConfigSource();
        configSource.set("format", "%d/%b/%Y:%T %z");
        configSource.set("timezone", task.getDefaultTimeZone());
        final TimestampParser time_parser = new TimestampParser(
            Exec.newConfigSource().loadConfig(ParserIntlTask.class),
            configSource.loadConfig(ParserIntlColumnOption.class));

        while( input.nextFile() ){
            while(true){
              line = lineDecoder.poll();

              if( line == null ){
                  break;
              }
              accessLogEntryMatcher = accessLogPattern.matcher(line);

              if(!accessLogEntryMatcher.matches()){
                throw new RuntimeException("unmatched line" + line);
              }

              pageBuilder.setString(0,accessLogEntryMatcher.group(1));
              pageBuilder.setString(1,accessLogEntryMatcher.group(2));
              pageBuilder.setString(2,accessLogEntryMatcher.group(3));
              try {
                  pageBuilder.setTimestamp(3,time_parser.parse(accessLogEntryMatcher.group(4)));
              } catch(TimestampParseException ex) {
                throw Throwables.propagate(ex);
              }
              pageBuilder.setString(4,accessLogEntryMatcher.group(5));
              pageBuilder.setString(5,accessLogEntryMatcher.group(6));
              pageBuilder.setString(6,accessLogEntryMatcher.group(7));
              pageBuilder.setString(7,accessLogEntryMatcher.group(8));
              pageBuilder.setString(8,accessLogEntryMatcher.group(9));
              if( format == LogFormat.combined ){
                  pageBuilder.setString(9,accessLogEntryMatcher.group(10));
                  pageBuilder.setString(10,accessLogEntryMatcher.group(11));
              }
              pageBuilder.addRecord();
            }
        }
        pageBuilder.finish();
    }

    private String getAccessLogRegex(LogFormat type)
    {
        final String rexa = "(\\d+(?:\\.\\d+){3})";  // an IP address
        final String rexs = "(\\S+)";                // a single token (no spaces)
        final String rexdt = "\\[([^\\]]+)\\]";      // something between [ and ]
        final String rexstr = "\"(.*?)\"";       // a quoted string
        final String rexi = "(\\d+)";                // unsigned integer
        final String rexp = "\"(\\S+)\\s(.*?)\\s(HTTP\\/\\d+\\.\\d+)\""; // method, path, protocol

        String rex;

        if( type == LogFormat.combined ){
          rex = "^" + String.join( " ", rexa, rexs, rexs, rexdt, rexp,
                             rexi, rexs, rexstr, rexstr) + "$";
        } else {
          rex = "^" + String.join( " ", rexa, rexs, rexs, rexdt, rexp,
                             rexi, rexs) + "$";
        }

        return rex;
   }
}
