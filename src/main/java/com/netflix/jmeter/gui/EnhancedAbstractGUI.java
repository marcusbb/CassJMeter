package com.netflix.jmeter.gui;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.sampler.Connection;
import com.netflix.jmeter.sampler.EnhancedAbstractSampler;

/**
 * Abstracted this so I could gain access to the private fields, to add a listener to the keyserializer jcomponent,
 * also removed the adding of the keyserializer so that there is more control at the extended layers on the layout. 
 *  
 * @author dvitorino
 *
 */
public abstract class EnhancedAbstractGUI extends AbstractSamplerGui
{
    private static final long serialVersionUID = -1372154378991423872L;
    private static final String WIKI = "https://github.com/Netflix/CassJMeter";

    public EnhancedAbstractGUI()
    {
        setLayout(new BorderLayout(0, 5));
        setBorder(makeBorder());
        add(AbstractGUI.addHelpLinkToPanel(makeTitlePanel(), WIKI), BorderLayout.NORTH);
        JPanel mainPanel = new JPanel(new GridBagLayout());
        GridBagConstraints labelConstraints = new GridBagConstraints();
        labelConstraints.anchor = GridBagConstraints.FIRST_LINE_END;

        GridBagConstraints editConstraints = new GridBagConstraints();
        editConstraints.anchor = GridBagConstraints.FIRST_LINE_START;
        editConstraints.weightx = 1.0;
        editConstraints.fill = GridBagConstraints.HORIZONTAL;
        
        init(mainPanel, labelConstraints, editConstraints);
        
        JPanel container = new JPanel(new BorderLayout());
        container.add(mainPanel, BorderLayout.NORTH);
        add(container, BorderLayout.CENTER);
        
    }

    @Override
    public void clearGui()
    {
        super.clearGui();
       
        initFields();
        if (Connection.connection != null)
        {
        	Connection.getInstance().shutdown();
        }
    }

    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
    }
    
    protected void configureTestElement(TestElement mc) 
    {
        super.configureTestElement(mc);       
    }
    
    public void addToPanel(JPanel panel, GridBagConstraints constraints, int col, int row, JComponent component)
    {
        constraints.gridx = col;
        constraints.gridy = row;
        
        panel.add(component, constraints);
    }

    @Override
    public String getStaticLabel()
    {
        return getLabel();
    }

    @Override
    public String getLabelResource()
    {
        return getLabel();
    }

    public abstract String getLabel();

    public abstract void initFields();

    public abstract void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints);
}
