package org.dcache.webadmin.view.pages.celladmin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.wicket.ajax.AjaxRequestTarget;
import org.apache.wicket.ajax.form.AjaxFormComponentUpdatingBehavior;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.basic.MultiLineLabel;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.DropDownChoice;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.AbstractReadOnlyModel;
import org.apache.wicket.model.PropertyModel;
import org.dcache.webadmin.controller.CellAdminService;
import org.dcache.webadmin.controller.exceptions.CellAdminServiceException;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.util.DefaultFocusBehaviour;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * The cellAdmin Webpage
 * @author jans
 */
public class CellAdmin extends BasePage implements AuthenticatedWebPage {

    private static final String EMPTY_STRING = "";
    private static final Logger _log = LoggerFactory.getLogger(CellAdmin.class);
    private Map<String, List<String>> _domainMap = new HashMap<String, List<String>>();
    private String _selectedDomain;
    private String _selectedCell;
    private String _command = "";
    private String _lastCommand = "";
    private String _response = "";

    public CellAdmin() {
        initDomainMap();
        addMarkup();
    }

    private void initDomainMap() {
        try {
            _domainMap = getCellAdminService().getDomainMap();
        } catch (CellAdminServiceException e) {
            error(getStringResource("error.noCells"));
            _log.error("could not retrieve cells: {}", e.getMessage());
        }
    }

    private void addMarkup() {
        Form cellAdminForm = new Form("cellAdminForm");
        final DropDownChoice domains = new DropDownChoice("cellAdminDomain",
                new PropertyModel(this, "_selectedDomain"),
                new DomainsModel());
        cellAdminForm.add(domains);
        final DropDownChoice cells = new DropDownChoice("cellAdminCell",
                new PropertyModel(this, "_selectedCell"),
                new CellsModel());
        cells.setRequired(true);
        cells.setOutputMarkupId(true);
        cellAdminForm.add(cells);
        domains.add(new AjaxFormComponentUpdatingBehavior("onchange") {

            @Override
            protected void onUpdate(AjaxRequestTarget target) {
                if (target != null) {
                    target.addComponent(cells);
                } else {
//                implement fallback for non javascript clients
                    cells.updateModel();
                }
            }
        });
        cellAdminForm.add(new FeedbackPanel("feedback"));
        TextField commandInput = new TextField("commandText",
                new PropertyModel(this, "_command"));
        commandInput.add(new DefaultFocusBehaviour());
        commandInput.setRequired(true);
        cellAdminForm.add(commandInput);
        cellAdminForm.add(new SubmitButton("submit"));
        cellAdminForm.add(new Label("lastCommand",
                new PropertyModel(this, "_lastCommand")));
        cellAdminForm.add(new Label("cellAdmin.receiver",
                new ReceiverModel()));
        cellAdminForm.add(new MultiLineLabel("cellAdmin.cellresponsevalue",
                new PropertyModel(this, "_response")));
        add(cellAdminForm);
    }

    private void clearResponse() {
        _response = EMPTY_STRING;
    }

    private CellAdminService getCellAdminService() {
        return getWebadminApplication().getCellAdminService();
    }

    private class SubmitButton extends Button {

        public SubmitButton(String id) {
            super(id);
        }

        @Override
        public void onSubmit() {
            try {
                String target = _selectedCell + "@" +
                        _selectedDomain;
                _log.debug("submit pressed with cell {} and command {}",
                        target, _command);
                _lastCommand = _command;
                clearResponse();
                _response = getCellAdminService().sendCommand(target, _command);
            } catch (CellAdminServiceException e) {
                _log.error("couldn't send CellCommand, {}",
                        e.getMessage());
                error(getStringResource("error.failure"));
            }
        }
    }

    private class DomainsModel extends AbstractReadOnlyModel<List<String>> {

        @Override
        public List<String> getObject() {
            return new ArrayList<String>(_domainMap.keySet());
        }
    }

    private class CellsModel extends AbstractReadOnlyModel<List<String>> {

        @Override
        public List<String> getObject() {
            List<String> cells = _domainMap.get(_selectedDomain);
            if (cells == null) {
                cells = Collections.EMPTY_LIST;
            }
            return cells;
        }
    }

    private class ReceiverModel extends AbstractReadOnlyModel<String> {

        @Override
        public String getObject() {
            if (_selectedCell != null && _selectedDomain != null) {
                return _selectedCell + "@" + _selectedDomain;
            }
            return "";
        }
    }
}