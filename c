wn completed.","stack_trace":""}
2026-08-04 16:50:59.231 INFO  [main] o.a.j.l.DirectJDKLog: Stopping service [Tomcat] 
{"@timestamp":"2026-08-04T16:50:59.2316921+05:30","level":"INFO","service":"ReportService","traceId":"","userId":"","clientIp":"","apiPath":"","class":"o.apache.catalina.core.StandardService","message":"Stopping service [Tomcat]","stack_trace":""}
2026-08-04 16:50:59.265 INFO  [main] o.s.b.a.l.ConditionEvaluationReportLogger: 

Error starting ApplicationContext. To display the condition evaluation report re-run your application with 'debug' enabled. 
{"@timestamp":"2026-08-04T16:50:59.2650715+05:30","level":"INFO","service":"ReportService","traceId":"","userId":"","clientIp":"","apiPath":"","class":"o.s.b.a.l.ConditionEvaluationReportLogger","message":"\r\n\r\nError starting ApplicationContext. To display the condition evaluation report re-run your application with 'debug' enabled.","stack_trace":""}
2026-08-04 16:50:59.283 ERROR [main] o.s.b.SpringApplication: Application run failed 
org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'hadoopConfig': Injection of autowired dependencies failed
        at org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor.postProcessProperties(AutowiredAnnotationBeanPostProcessor.java:499)
        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.populateBean(AbstractAutowireCapableBeanFactory.java:1444)
        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:602)
        at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:525)
        at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:333)
        at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:371)
        at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:331)
        at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:196)
        at org.springframework.beans.factory.support.DefaultListableBeanFactory.instantiateSingleton(DefaultListableBeanFactory.java:1225)
        at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingleton(DefaultListableBeanFactory.java:1191)
        at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:1121)
        at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:994)
        at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:621)
        at org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext.refresh(ServletWebServerApplicationContext.java:143)
        at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:756)
        at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:445)
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:321)
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:1365)
        at org.springframework.boot.SpringApplication.run(SpringApplication.java:1354)
        at com.fincore.ReportService.ReportServiceApplication.main(ReportServiceApplication.java:10)
Caused by: org.springframework.util.PlaceholderResolutionException: Could not resolve placeholder 'hadoop.fs.uri' in value "${hadoop.fs.uri}"
        at org.springframework.util.PlaceholderResolutionException.withValue(PlaceholderResolutionException.java:81)
        at org.springframework.util.PlaceholderParser$ParsedValue.resolve(PlaceholderParser.java:296)
        at org.springframework.util.PlaceholderParser.replacePlaceholders(PlaceholderParser.java:129)
        at org.springframework.util.PropertyPlaceholderHelper.replacePlaceholders(PropertyPlaceholderHelper.java:96)
        at org.springframework.core.env.AbstractPropertyResolver.doResolvePlaceholders(AbstractPropertyResolver.java:286)
        at org.springframework.core.env.AbstractPropertyResolver.resolveRequiredPlaceholders(AbstractPropertyResolver.java:257)
        at org.springframework.context.support.PropertySourcesPlaceholderConfigurer.lambda$processProperties$0(PropertySourcesPlaceholderConfigurer.java:184)
        at org.springframework.beans.factory.support.AbstractBeanFactory.resolveEmbeddedValue(AbstractBeanFactory.java:959)
        at org.springframework.beans.factory.support.DefaultListableBeanFactory.doResolveDependency(DefaultListableBeanFactory.java:1679)
        at org.springframework.beans.factory.support.DefaultListableBeanFactory.resolveDependency(DefaultListableBeanFactory.java:1658)
        at org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor$AutowiredFieldElement.resolveFieldValue(AutowiredAnnotationBeanPostProcessor.java:764)
        at org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor$AutowiredFieldElement.inject(AutowiredAnnotationBeanPostProcessor.java:748)
        at org.springframework.beans.factory.annotation.InjectionMetadata.inject(InjectionMetadata.java:146)
        at org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor.postProcessProperties(AutowiredAnnotationBeanPostProcessor.java:493)
        ... 19 common frames omitted
{"@timestamp":"2026-08-04T16:50:59.2837188+05:30","level":"ERROR","service":"ReportService","traceId":"","userId":"","clientIp":"","apiPath":"","class":"o.springframework.boot.SpringApplication","message":"Application run failed","stack_trace":"org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'hadoopConfig': Injection of autowired dependencies failed\r\n\tat org.springframework.beans.factory.annotation.AutowiredAnnotationBeanPostProcessor.postProcessProperties(AutowiredAnnotationBeanPostProcessor.java:499)\r\n\tat org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.populateBean(AbstractAutowireCapableBeanFactory.java:1444)\r\n\tat org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:602)\r\n\tat org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:525)\r\n\tat org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:333)\r\n\tat org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:371)\r\n\tat org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:331)\r\n\tat org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:196)\r\n\tat org.springframework.beans.factory.support.DefaultListableBeanFactory.instantiateSingleton(DefaultListableBeanFactory.java:1225)\r\n\tat org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingleton(DefaultListableBeanFactory.java:1191)\r\nCaused by: org.springframework.util.PlaceholderResolutionException: Could not resolve placeholder 'hadoop.fs.uri' in value \"${hadoop.fs.uri}\"\r\n\tat org.springframework.util.PlaceholderResolutionException.withValue(PlaceholderResolutionException.java:81)\r\n\tat org.springframework.util.PlaceholderParser$ParsedValue.resolve(PlaceholderParser.java:296)\r\n\tat org.springframework.util.PlaceholderParser.replacePlaceholders(PlaceholderParser.java:129)\r\n\tat org.springframework.util.PropertyPlaceholderHelper.replacePlaceholders(PropertyPlaceholderHelper.java:96)\r\n\tat org.springframework.core.env.AbstractPropertyResolver.doResolvePlaceholders(AbstractPropertyResolver.java:286)\r\n\tat org.springframework.core.env.AbstractPropertyResolver.resolveRequiredPlaceholders(AbstractPropertyResolver.java:257)\r\n\tat org.springframework.context.support.PropertySourcesPlaceholderConfigurer.lambda$processProperties$0(PropertySourcesPlaceholderConfigurer.java:184)\r\n\tat org.springframework.beans.factory.support.AbstractBeanFactory.resolveEmbeddedValue(AbstractBeanFactory.java:959)\r\n\tat org.springframework.beans.factory.support.DefaultListableBeanFactory.doResolveDependency(DefaultListableBeanFactory.java:1679)\r\n\tat org.springframework.beans.factory.support.DefaultListableBeanFactory.resolveDependency(DefaultListableBeanFactory.java:1658)\r\n"}
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  38.976 s
[INFO] Finished at: 2026-08-04T16:51:00+05:30
[INFO] ------------------------------------------------------------------------
[ERROR] Failed to execute goal org.springframework.boot:spring-boot-maven-plugin:4.1.0:run (default-cli) on project ReportService: Process terminated with exit code: 1 -> [Help 1]
[ERROR] 
[ERROR] To see the full stack trace of the errors, re-run Maven with the -e switch.
[ERROR] Re-run Maven using the -X switch to enable full debug logging.
[ERROR] 
[ERROR] For more information about the errors and possible solutions, please read the following articles:
[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/MojoExecutionException

D:\Workspace\Git\Fincore\Backend\fincore_report_service
