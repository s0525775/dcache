package org.dcache.webadmin.view.pages.login;

import org.apache.wicket.Page;
import org.apache.wicket.Session;
import org.apache.wicket.authentication.IAuthenticationStrategy;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.CheckBox;
import org.apache.wicket.markup.html.form.PasswordTextField;
import org.apache.wicket.markup.html.form.StatelessForm;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.CompoundPropertyModel;
import org.apache.wicket.protocol.http.servlet.ServletWebRequest;
import org.apache.wicket.protocol.https.RequireHttps;
import org.apache.wicket.request.RequestHandlerStack.ReplaceHandlerException;
import org.apache.wicket.request.cycle.RequestCycle;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;

import java.security.cert.X509Certificate;

import org.dcache.webadmin.controller.LogInService;
import org.dcache.webadmin.controller.exceptions.LogInServiceException;
import org.dcache.webadmin.view.beans.LogInBean;
import org.dcache.webadmin.view.beans.UserBean;
import org.dcache.webadmin.view.beans.WebAdminInterfaceSession;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.panels.userpanel.UserPanel;
import org.dcache.webadmin.view.util.DefaultFocusBehaviour;

@RequireHttps
public class LogIn extends BasePage {

    private static final String X509_CERTIFICATE_ATTRIBUTE
        = "javax.servlet.request.X509Certificate";
    private static final Logger _log = LoggerFactory.getLogger(LogIn.class);
    private static final long serialVersionUID = 8902191632839916396L;

    private Class<? extends Page> returnPage;

    private class LogInForm extends StatelessForm {

        private class CertSignInButton extends Button {

            private static final long serialVersionUID = 7349334961548160283L;

            public CertSignInButton(String id) {
                super(id);
                /*
                 * deactivate checking of formdata for certsignin
                 */
                this.setDefaultFormProcessing(false);
            }

            @Override
            public void onSubmit() {
                try {
                    if (!isSignedIn()) {
                        signInWithCert(getLogInService());
                    }
                    setGoOnPage();
                } catch (IllegalArgumentException ex) {
                    error(getStringResource("noCertError"));
                    _log.debug("no certificate provided");
                } catch (LogInServiceException ex) {
                    String cause = "unknown";
                    if (ex.getMessage() != null) {
                        cause = ex.getMessage();
                    }
                    error(getStringResource("loginError"));
                    _log.debug("cert sign in error - cause {}", cause);
                }
            }
        }

        private class LogInButton extends Button {

            private static final long serialVersionUID = -8852712258475979167L;

            public LogInButton(String id) {
                super(id);
            }

            @Override
            public void onSubmit() {
                IAuthenticationStrategy strategy
                    = getApplication().getSecuritySettings()
                                      .getAuthenticationStrategy();
                try {
                    if (!isSignedIn()) {
                        signIn(_logInModel, strategy);
                    }
                    setGoOnPage();
                } catch (LogInServiceException ex) {
                    strategy.remove();
                    String cause = "unknown";
                    if (ex.getMessage() != null) {
                        cause = ex.getMessage();
                    }
                    error(getStringResource("loginError") + " - cause: "
                                    + cause);
                    _log.debug("user/pwd sign in error - cause {}", cause);
                }
            }
        }

        private static final long serialVersionUID = -1800491058587279179L;
        private TextField _username;
        private PasswordTextField _password;
        private CheckBox _rememberMe;
        private WebMarkupContainer _rememberMeRow;
        private LogInBean _logInModel;

        LogInForm(final String id) {
            super(id, new CompoundPropertyModel<>(new LogInBean()));
            _logInModel = (LogInBean) getDefaultModelObject();

            _username = new TextField("username");
            _username.setRequired(true);
            add(_username);
            _password = new PasswordTextField("password");

            add(_password);
            add(new LogInButton("submit"));
            _rememberMeRow = new WebMarkupContainer("rememberMeRow");
            add(_rememberMeRow);
            _rememberMe = new CheckBox("remembering");
            _rememberMeRow.add(_rememberMe);
            Button certButton = new CertSignInButton("certsignin");
            certButton.add(new DefaultFocusBehaviour());
            add(certButton);
        }

        private LogInService getLogInService() {
            return getWebadminApplication().getLogInService();
        }

        private boolean isSignedIn() {
            return getWebadminSession().isSignedIn();
        }

        /**
         * There seems to be an unresolved issue in the way the base url
         * is determined for the originating page.  It seems that if the
         * RestartResponseAtInterceptPageException is thrown
         * from within a subcomponent of the page, the url
         * reflects in its query part that subcomponent, and
         * consequently on redirect after intercept, Wicket attempts
         * to return to it, generating an Access Denied Page.
         * <p>
         * At present I see no other way of maintaining proper intercept-
         * redirect behavior except via a workaround which defeats
         * the exception handling mechanism by setting the response page,
         * determined via the constructor.  Panels using the
         * intercept exception (for example, {@link UserPanel}) should
         * set the first page parameter to be the originating page
         * class name.
         * <p>
         * This behavior has been present since Wicket 1.5.
         * See <a href="http://apache-wicket.1842946.n4.nabble.com/RestartResponseAtInterceptPageException-problem-in-1-5-td4255020.html">RestartAtIntercept problem</a>.
         */
        private void setGoOnPage() {
            /*
             * If login has been called because the user was not yet logged in,
             * then continue to the original destination, otherwise to the Home
             * page.
             */
            try {
                continueToOriginalDestination();
            } catch (ReplaceHandlerException e) {
                /*
                 * Note that #continueToOriginalDestination should use
                 * this exception to return to the calling page, with
                 * no exception thrown if this page was not invoked
                 * as an intercept page.  Hence catching this exception
                 * is technically incorrect behavior, according to the
                 * Wicket specification.  But see the documentation
                 * to this method.
                 */
            }

            setResponsePage(returnPage);
        }

        private void signIn(LogInBean model, IAuthenticationStrategy strategy)
                        throws LogInServiceException {
            String username;
            String password;
            if (model != null) {
                username = model.getUsername();
                password = model.getPassword();
            } else {
                /*
                 * get username and password from persistence store
                 */
                String[] data = strategy.load();
                if ((data == null) || (data.length <= 1)) {
                    throw new LogInServiceException("no username data saved");
                }
                username = data[0];
                password = data[1];
            }
            _log.debug("username sign in, username: {}", username);
            UserBean user = getLogInService().authenticate(username,
                            password.toCharArray());
            getWebadminSession().setUser(user);
            if (model != null && model.isRemembering()) {
                strategy.save(username, password);
            } else {
                strategy.remove();
            }
        }
    }

    public LogIn() {
        super();
        this.returnPage = getApplication().getHomePage();
    }

    public LogIn(PageParameters pageParameters) throws ClassNotFoundException {
        super(pageParameters);
        this.returnPage = (Class<? extends Page>)BasePage.class.getClassLoader()
                           .loadClass(pageParameters.get(0).toString());
    }

    protected void initialize() {
        super.initialize();
        final FeedbackPanel feedback = new FeedbackPanel("feedback");
        add(new Label("dCacheInstanceName",
                        getWebadminApplication().getDcacheName()));
        add(feedback);
        add(new LogInForm("LogInForm"));
    }

    public static void signInWithCert(LogInService service)
                    throws IllegalArgumentException,
                    LogInServiceException {
        X509Certificate[] certChain = getCertChain();
        UserBean user = service.authenticate(certChain);
        WebAdminInterfaceSession session = (WebAdminInterfaceSession) Session.get();
        session.setUser(user);
    }

    private static X509Certificate[] getCertChain() {
        ServletWebRequest servletWebRequest
            = (ServletWebRequest) RequestCycle.get().getRequest();
        HttpServletRequest request = servletWebRequest.getContainerRequest();
        Object certificate = request.getAttribute(X509_CERTIFICATE_ATTRIBUTE);
        X509Certificate[] chain;
        if (certificate instanceof X509Certificate[]) {
            chain = (X509Certificate[]) certificate;
        } else {
            throw new IllegalArgumentException();
        }
        return chain;
    }
}
