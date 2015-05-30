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
            extends Task, LineDecoder.DecoderTask, TimestampParser.ParserTask
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

        columns.add(new ColumnConfig("remote_host",STRING ,null));
        columns.add(new ColumnConfig("identity_check",STRING ,null));
        columns.add(new ColumnConfig("user",STRING ,null));
        columns.add(new ColumnConfig("datetime",TIMESTAMP,null));
        columns.add(new ColumnConfig("method",STRING ,null));
        columns.add(new ColumnConfig("path",STRING ,null));
        columns.add(new ColumnConfig("protocol",STRING ,null));
        columns.add(new ColumnConfig("status",STRING ,null));
        columns.add(new ColumnConfig("size",STRING ,null));

        // combined
        if( format == LogFormat.combined ){
          columns.add(new ColumnConfig("referer",STRING ,null));
          columns.add(new ColumnConfig("user_agent",STRING ,null));
        }

        Schema schema = new SchemaConfig(columns).toSchema();
        control.run(task.dump(), schema);
    }

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
        final TimestampParser time_parser = new TimestampParser("%d/%b/%Y:%T %z",task);

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
        final String rexstr = "\"([^\"]*?)\"";       // a quoted string
        final String rexi = "(\\d+)";                // unsigned integer
        final String rexp = "\"(\\S+)\\s(\\S+)\\s(\\S+)\""; // method, path, protocol
        String rex;

        if( type == LogFormat.combined ){
          rex = "^" + String.join( " ", rexa, rexs, rexs, rexdt, rexp,
                             rexi, rexi, rexstr, rexstr) + "$";
        } else {
          rex = "^" + String.join( " ", rexa, rexs, rexs, rexdt, rexp,
                             rexi, rexi) + "$";
        }

        return rex;
   }
}
