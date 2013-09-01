package org.dcache.cdmi;

import javax.servlet.ServletContext;
import org.eclipse.jetty.server.Server;
import org.springframework.web.context.ServletContextAware;

public class ServerConfigurer extends Server implements ServletContextAware
{
    private ServletContext servletContext = null;

    public static final String ATTRIBUTE_NAME_CONTAINER = "org.dcache.cdmi.container";

    @Override
    protected void doStart() throws Exception
    {
        //getServer().setAttribute(ATTRIBUTE_NAME_CONTAINER, "test");
    }

    @Override
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
        servletContext.setAttribute(ATTRIBUTE_NAME_CONTAINER, "test");
    }
}
