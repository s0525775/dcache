/*
 * Copyright (c) 2010, Oracle
 * Copyright (c) 2010, The Storage Networking Industry Association.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of The Storage Networking Industry Association (SNIA) nor
 * the names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *  THE POSSIBILITY OF SUCH DAMAGE.
 */


import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpEntity;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import java.security.KeyStore;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * REMARK (from Jana):
 * The CDMItest may also tell "success" if all tests fail. Please also check the 'Output' window
 * in NetBeans IDE for CDMItest and the dCacheDomain.log file for error messages - always! A test
 * is successful if no relevant error messages appeared in the 'Output' window of NetBeans and in
 * the dCacheDomain.log file. Since the current dcache-cdmi version is still not stable, the
 * CDMItest still needs a HelperClass which causes a break of 3 seconds between every test. Tests
 * can be included by the '@Test' annotation and excluded by the '@Ignore' annotation. This example
 * of CDMItest will include the first 5 tests and exclude the last 2 tests. dCacheDomain.log is
 * principally used for investigating problems and tests. dcache-cdmi also generates .log files
 * below the /tmp directory at the moment which is still necessary for some tests. The .log files
 * will be removed later again when the corresponding source code part is fully implemented.
 * Those .log files don't get deleted automatically and may need to get deleted manually later.
 * The port in CDMItest can be replaced by a variable, or by using the replace function of NetBeans.
 */

/**
 *
 * @author Mark A. Carlson
 */
public class CDMITlsX509Test {
    private final static String KEYSTORE = "my.certificate";
    private final static String TRUSTSTORE = "my.certificate";
    private final static String KEYSTORE_PASSWORD = "nopassword";
    private final static String TRUSTSTORE_PASSWORD = "nopassword";

    public static class HelperClass {
        public static void sleep(long ms) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException ex) {
                Logger.getLogger(CDMITlsX509Test.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    @Test
    public void testCapabilities() throws Exception {
        HelperClass.sleep(3000);
        HttpClient httpclient = new DefaultHttpClient();

        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            FileInputStream keystoreInput = new FileInputStream(new File(KEYSTORE));
            keystore.load(keystoreInput, KEYSTORE_PASSWORD.toCharArray());
            KeyStore truststore = KeyStore.getInstance("PKCS12");
            FileInputStream truststoreIs = new FileInputStream(new File(TRUSTSTORE));
            truststore.load(truststoreIs, TRUSTSTORE_PASSWORD.toCharArray());
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(keystore, KEYSTORE_PASSWORD, truststore);
            Scheme sch = new Scheme("https", sslSocketFactory, 8543);
            httpclient.getConnectionManager().getSchemeRegistry().register(sch);
            //HttpParams httpParams = new BasicHttpParams();
            //SchemeRegistry schemeRegistry = new SchemeRegistry();
            //schemeRegistry.register(sch);
            //HttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(httpParams, schemeRegistry), httpParams);

            // Create the request
            HttpResponse response = null;
            HttpGet httpget = new HttpGet("http://localhost:8543/cdmi_capabilities");
            httpget.setHeader("Accept", "application/cdmi-capability");
            httpget.setHeader("X-CDMI-Specification-Version", "1.0.2");
            response = httpclient.execute(httpget);

            Header[] hdr = response.getAllHeaders();
            System.out.println("Headers : " + hdr.length);
            for (int i = 0; i < hdr.length; i++) {
                System.out.println(hdr[i]);
            }
            System.out.println("---------");
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());

            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
            System.out.println("---------");
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                long len = entity.getContentLength();
                if (len != -1 && len < 2048) {
                    System.out.println(EntityUtils.toString(entity));
                }
            }

        } catch (Exception ex) {
            System.out.println(ex);
        }// exception
    }

    @Test
    public void testContainerCreate() throws Exception {
        HelperClass.sleep(3000);
        HttpClient httpclient = new DefaultHttpClient();

        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            FileInputStream keystoreInput = new FileInputStream(new File(KEYSTORE));
            keystore.load(keystoreInput, KEYSTORE_PASSWORD.toCharArray());
            KeyStore truststore = KeyStore.getInstance("PKCS12");
            FileInputStream truststoreIs = new FileInputStream(new File(TRUSTSTORE));
            truststore.load(truststoreIs, TRUSTSTORE_PASSWORD.toCharArray());
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(keystore, KEYSTORE_PASSWORD, truststore);
            Scheme sch = new Scheme("https", sslSocketFactory, 8543);
            httpclient.getConnectionManager().getSchemeRegistry().register(sch);
            //HttpParams httpParams = new BasicHttpParams();
            //SchemeRegistry schemeRegistry = new SchemeRegistry();
            //schemeRegistry.register(sch);
            //HttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(httpParams, schemeRegistry), httpParams);

            // Create the request
            HttpResponse response = null;
            HttpPut httpput = new HttpPut("http://localhost:8543/TestContainer");
            httpput.setHeader("Content-Type", "application/cdmi-container");
            httpput.setHeader("X-CDMI-Specification-Version", "1.0.2");
            //httpput.setEntity(new StringEntity("{ \"metadata\" : { } }"));
            httpput.setEntity(new StringEntity("{ \"metadata\" : { \"color\" : \"red\", \"test\" : \"Test\" } }"));
            response = httpclient.execute(httpput);

            Header[] hdr = response.getAllHeaders();
            System.out.println("Headers : " + hdr.length);
            for (int i = 0; i < hdr.length; i++) {
                System.out.println(hdr[i]);
            }
            System.out.println("---------");
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            Assert.assertEquals(201, response.getStatusLine().getStatusCode());

            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
            System.out.println("---------");
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                long len = entity.getContentLength();
                if (len != -1 && len < 2048) {
                    System.out.println(EntityUtils.toString(entity));
                }
            }

        } catch (Exception ex) {
            System.out.println(ex);
        }// exception
    }

    @Test
    public void testContainerUpdate() throws Exception {
        HelperClass.sleep(3000);
        HttpClient httpclient = new DefaultHttpClient();

        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            FileInputStream keystoreInput = new FileInputStream(new File(KEYSTORE));
            keystore.load(keystoreInput, KEYSTORE_PASSWORD.toCharArray());
            KeyStore truststore = KeyStore.getInstance("PKCS12");
            FileInputStream truststoreIs = new FileInputStream(new File(TRUSTSTORE));
            truststore.load(truststoreIs, TRUSTSTORE_PASSWORD.toCharArray());
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(keystore, KEYSTORE_PASSWORD, truststore);
            Scheme sch = new Scheme("https", sslSocketFactory, 8543);
            httpclient.getConnectionManager().getSchemeRegistry().register(sch);
            //HttpParams httpParams = new BasicHttpParams();
            //SchemeRegistry schemeRegistry = new SchemeRegistry();
            //schemeRegistry.register(sch);
            //HttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(httpParams, schemeRegistry), httpParams);

            // Create the request
            HttpResponse response = null;
            HttpPut httpput = new HttpPut("http://localhost:8543/TestContainer/");
            httpput.setHeader("Content-Type", "application/cdmi-container");
            httpput.setHeader("X-CDMI-Specification-Version", "1.0.2");
            //httpput.setEntity(new StringEntity("{ \"metadata\" : { } }"));
            httpput.setEntity(new StringEntity("{ \"metadata\" : { \"color\" : \"green\", \"test\" : \"Test\" } }"));
            response = httpclient.execute(httpput);

            Header[] hdr = response.getAllHeaders();
            System.out.println("Headers : " + hdr.length);
            for (int i = 0; i < hdr.length; i++) {
                System.out.println(hdr[i]);
            }
            System.out.println("---------");
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            Assert.assertEquals(201, response.getStatusLine().getStatusCode());

            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
            System.out.println("---------");
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                long len = entity.getContentLength();
                if (len != -1 && len < 2048) {
                    System.out.println(EntityUtils.toString(entity));
                }
            }

        } catch (Exception ex) {
            System.out.println(ex);
        }// exception
    }

    @Test
    public void testObjectCreate() throws Exception {
        HelperClass.sleep(3000);
        HttpClient httpclient = new DefaultHttpClient();

        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            FileInputStream keystoreInput = new FileInputStream(new File(KEYSTORE));
            keystore.load(keystoreInput, KEYSTORE_PASSWORD.toCharArray());
            KeyStore truststore = KeyStore.getInstance("PKCS12");
            FileInputStream truststoreIs = new FileInputStream(new File(TRUSTSTORE));
            truststore.load(truststoreIs, TRUSTSTORE_PASSWORD.toCharArray());
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(keystore, KEYSTORE_PASSWORD, truststore);
            Scheme sch = new Scheme("https", sslSocketFactory, 8543);
            httpclient.getConnectionManager().getSchemeRegistry().register(sch);
            //HttpParams httpParams = new BasicHttpParams();
            //SchemeRegistry schemeRegistry = new SchemeRegistry();
            //schemeRegistry.register(sch);
            //HttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(httpParams, schemeRegistry), httpParams);

            // Create the request
            HttpResponse response = null;
            HttpPut httpput = new HttpPut(
                    "http://localhost:8543/TestContainer/TestObject.txt");
            httpput.setHeader("Content-Type", "application/cdmi-object");
            httpput.setHeader("X-CDMI-Specification-Version", "1.0.2");
            String respStr = "{\n";
            respStr = respStr + "\"mimetype\" : \"" + "text/plain" + "\",\n";
            respStr = respStr + "\"value\" : \"" + "This is a test" + "\",\n";
            respStr = respStr + "\"metadata\" : {" + "\"color\" : \"yellow\", \"test\" : \"Test2\"" + "}\n";
            respStr = respStr + "}\n";
            System.out.println(respStr);
            StringEntity entity = new StringEntity(respStr);
            httpput.setEntity(entity);
            response = httpclient.execute(httpput);

            Header[] hdr = response.getAllHeaders();
            System.out.println("Headers : " + hdr.length);
            for (int i = 0; i < hdr.length; i++) {
                System.out.println(hdr[i]);
            }
            System.out.println("---------");
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            Assert.assertEquals(201, response.getStatusLine().getStatusCode());

            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
            System.out.println("---------");

        } catch (Exception ex) {
            System.out.println(ex);
        }// exception
    }

    @Test
    public void testObjectUpdate() throws Exception {
        HelperClass.sleep(3000);
        HttpClient httpclient = new DefaultHttpClient();

        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            FileInputStream keystoreInput = new FileInputStream(new File(KEYSTORE));
            keystore.load(keystoreInput, KEYSTORE_PASSWORD.toCharArray());
            KeyStore truststore = KeyStore.getInstance("PKCS12");
            FileInputStream truststoreIs = new FileInputStream(new File(TRUSTSTORE));
            truststore.load(truststoreIs, TRUSTSTORE_PASSWORD.toCharArray());
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(keystore, KEYSTORE_PASSWORD, truststore);
            Scheme sch = new Scheme("https", sslSocketFactory, 8543);
            httpclient.getConnectionManager().getSchemeRegistry().register(sch);
            //HttpParams httpParams = new BasicHttpParams();
            //SchemeRegistry schemeRegistry = new SchemeRegistry();
            //schemeRegistry.register(sch);
            //HttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(httpParams, schemeRegistry), httpParams);

            // Create the request
            HttpResponse response = null;
            HttpPut httpput = new HttpPut(
                    "http://localhost:8543/TestContainer/TestObject.txt");
            httpput.setHeader("Content-Type", "application/cdmi-object");
            httpput.setHeader("X-CDMI-Specification-Version", "1.0.2");
            String respStr = "{\n";
            respStr = respStr + "\"mimetype\" : \"" + "text/plain" + "\",\n";
            respStr = respStr + "\"value\" : \"" + "This is a new test" + "\",\n";
            respStr = respStr + "\"metadata\" : {" + "\"color\" : \"orange\", \"test\" : \"Test2\"" + "}\n";
            respStr = respStr + "}\n";
            System.out.println(respStr);
            StringEntity entity = new StringEntity(respStr);
            httpput.setEntity(entity);
            response = httpclient.execute(httpput);

            Header[] hdr = response.getAllHeaders();
            System.out.println("Headers : " + hdr.length);
            for (int i = 0; i < hdr.length; i++) {
                System.out.println(hdr[i]);
            }
            System.out.println("---------");
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            Assert.assertEquals(200, response.getStatusLine().getStatusCode());

            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
            System.out.println("---------");

        } catch (Exception ex) {
            System.out.println(ex);
        }// exception
    }

    @Test
    public void testObjectDelete() throws Exception {
        HelperClass.sleep(3000);
        HttpClient httpclient = new DefaultHttpClient();

        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            FileInputStream keystoreInput = new FileInputStream(new File(KEYSTORE));
            keystore.load(keystoreInput, KEYSTORE_PASSWORD.toCharArray());
            KeyStore truststore = KeyStore.getInstance("PKCS12");
            FileInputStream truststoreIs = new FileInputStream(new File(TRUSTSTORE));
            truststore.load(truststoreIs, TRUSTSTORE_PASSWORD.toCharArray());
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(keystore, KEYSTORE_PASSWORD, truststore);
            Scheme sch = new Scheme("https", sslSocketFactory, 8543);
            httpclient.getConnectionManager().getSchemeRegistry().register(sch);
            //HttpParams httpParams = new BasicHttpParams();
            //SchemeRegistry schemeRegistry = new SchemeRegistry();
            //schemeRegistry.register(sch);
            //HttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(httpParams, schemeRegistry), httpParams);

            // Create the request
            HttpResponse response = null;
            HttpDelete httpdelete = new HttpDelete(
                    "http://localhost:8543/TestContainer/TestObject.txt");
            httpdelete.setHeader("Content-Type", "application/cdmi-object");
            httpdelete.setHeader("X-CDMI-Specification-Version", "1.0.2");
            response = httpclient.execute(httpdelete);

            Header[] hdr = response.getAllHeaders();
            System.out.println("Headers : " + hdr.length);
            for (int i = 0; i < hdr.length; i++) {
                System.out.println(hdr[i]);
            }
            System.out.println("---------");
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            Assert.assertEquals(204, response.getStatusLine().getStatusCode());

            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
            System.out.println("---------");

        } catch (Exception ex) {
            System.out.println(ex);
        }// exception
    }

    @Test
    public void testContainerDelete() throws Exception {
        HelperClass.sleep(3000);
        HttpClient httpclient = new DefaultHttpClient();

        try {
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            FileInputStream keystoreInput = new FileInputStream(new File(KEYSTORE));
            keystore.load(keystoreInput, KEYSTORE_PASSWORD.toCharArray());
            KeyStore truststore = KeyStore.getInstance("PKCS12");
            FileInputStream truststoreIs = new FileInputStream(new File(TRUSTSTORE));
            truststore.load(truststoreIs, TRUSTSTORE_PASSWORD.toCharArray());
            SSLSocketFactory sslSocketFactory = new SSLSocketFactory(keystore, KEYSTORE_PASSWORD, truststore);
            Scheme sch = new Scheme("https", sslSocketFactory, 8543);
            httpclient.getConnectionManager().getSchemeRegistry().register(sch);
            //HttpParams httpParams = new BasicHttpParams();
            //SchemeRegistry schemeRegistry = new SchemeRegistry();
            //schemeRegistry.register(sch);
            //HttpClient httpClient = new DefaultHttpClient(new ThreadSafeClientConnManager(httpParams, schemeRegistry), httpParams);

            // Create the request
            HttpResponse response = null;
            HttpDelete httpdelete = new HttpDelete(
                    "http://localhost:8543/TestContainer");
            httpdelete.setHeader("Content-Type", "application/cdmi-container");
            httpdelete.setHeader("X-CDMI-Specification-Version", "1.0.2");
            response = httpclient.execute(httpdelete);

            Header[] hdr = response.getAllHeaders();
            System.out.println("Headers : " + hdr.length);
            for (int i = 0; i < hdr.length; i++) {
                System.out.println(hdr[i]);
            }
            System.out.println("---------");
            System.out.println(response.getProtocolVersion());
            System.out.println(response.getStatusLine().getStatusCode());
            Assert.assertEquals(204, response.getStatusLine().getStatusCode());

            System.out.println(response.getStatusLine().getReasonPhrase());
            System.out.println(response.getStatusLine().toString());
            System.out.println("---------");

        } catch (Exception ex) {
            System.out.println(ex);
        }// exception
    }
}
