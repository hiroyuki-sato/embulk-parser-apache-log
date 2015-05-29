Embulk::JavaPlugin.register_parser(
  "apache-log", "org.embulk.parser.ApacheLogParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
