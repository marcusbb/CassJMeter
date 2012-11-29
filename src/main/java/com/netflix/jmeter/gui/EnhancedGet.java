package com.netflix.jmeter.gui;

import java.awt.GridBagConstraints;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.testelement.TestElement;

import com.netflix.jmeter.gui.components.CompositeSerializerList;
import com.netflix.jmeter.sampler.EnhancedAbstractSampler;
import com.netflix.jmeter.sampler.EnhancedGetSampler;

public class EnhancedGet extends EnhancedAbstractGUI
{    
 	private static final long serialVersionUID = -2385526600604989215L;
	private static final String LABEL = "Cassandra Enhanced Get";    
	private JTextField CNAME;
    private JComboBox KSERIALIZER;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;
    
    private CompositeSerializerList COMPOSITE_KSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_CSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_VSERIALIZERS_LIST;
   
    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        CNAME.setText(element.getPropertyAsString(EnhancedGetSampler.COLUMN_NAME));
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedGetSampler.VALUE_SERIALIZER_TYPE));
        COMPOSITE_KSERIALIZERS_LIST.setListModelElements(EnhancedGetSampler.getAsArray(element.getPropertyAsString(EnhancedGetSampler.COMPOSITE_KEY_SERIALIZER_TYPES), ","));
        COMPOSITE_CSERIALIZERS_LIST.setListModelElements(EnhancedGetSampler.getAsArray(element.getPropertyAsString(EnhancedGetSampler.COMPOSITE_COLUMN_SERIALIZER_TYPES), ","));
        COMPOSITE_VSERIALIZERS_LIST.setListModelElements(EnhancedGetSampler.getAsArray(element.getPropertyAsString(EnhancedGetSampler.COMPOSITE_VALUE_SERIALIZER_TYPES), ","));       
    }
    
    public TestElement createTestElement()
    {
    	EnhancedGetSampler sampler = new EnhancedGetSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");     
        return sampler;
        
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof EnhancedGetSampler)
        {
        	EnhancedGetSampler gSampler = (EnhancedGetSampler) sampler;
        	gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setColumnName(CNAME.getText());            
                   
            if("CompositeSerializer".equals(gSampler.getKSerializerType())){
            	gSampler.setCompositeKSerializerTypes(COMPOSITE_KSERIALIZERS_LIST.getListModelAsArray());
            }
            
            if("CompositeSerializer".equals(gSampler.getCSerializerType())){
            	gSampler.setCompositeCSerializerTypes(COMPOSITE_CSERIALIZERS_LIST.getListModelAsArray());
            }
            
            if("CompositeSerializer".equals(gSampler.getVSerializerType())){
            	gSampler.setCompositeVSerializerTypes(COMPOSITE_VSERIALIZERS_LIST.getListModelAsArray());
            }
        }
    }

    public void initFields()
    {     	
        CNAME.setText("${__Random(1,1000)}");
        KSERIALIZER.setSelectedItem("Key Serializer");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
    }

    @Override
    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {
    	int y = 3;
        addToPanel(mainPanel, labelConstraints, 0, y, new JLabel("Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, CNAME = new JTextField());
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, KSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        
        addToPanel(mainPanel, editConstraints, 1, ++y, COMPOSITE_KSERIALIZERS_LIST = new CompositeSerializerList());
        KSERIALIZER.addActionListener(COMPOSITE_KSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, CSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        
        addToPanel(mainPanel, editConstraints, 1, ++y, COMPOSITE_CSERIALIZERS_LIST = new CompositeSerializerList());
        CSERIALIZER.addActionListener(COMPOSITE_CSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, ++y, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, y, VSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        
        addToPanel(mainPanel, editConstraints, 1, ++y, COMPOSITE_VSERIALIZERS_LIST = new CompositeSerializerList());
        VSERIALIZER.addActionListener(COMPOSITE_VSERIALIZERS_LIST);
        
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }    
}
