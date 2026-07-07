util files
package com.fincore.ReportService.util;

public class Constants {
     public static  String SUCCESS="SUCCESS";
    public static  String PARTIAL_SUCCESS="PARTIAL_SUCCESS";
    public static  String FAILED="FAILED";

}
package com.fincore.ReportService.util;

public enum FormatStatus {
    GENERATED,
    FAILED
}
package com.fincore.ReportService.util;

public enum ReportFormat {
    PDF(".pdf"),
    EXCEL(".xlsx"),
    PSV(".psv");
    private final String extension;
    ReportFormat(String extension) {
        this.extension = extension;
    }
    public String ext() {
        return extension;
    }
}

application file
package com.fincore.ReportService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ReportServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReportServiceApplication.class, args);
	}

}

package com.fincore.ReportService;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

public class ServletInitializer extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(ReportServiceApplication.class);
	}

}

