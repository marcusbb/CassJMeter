package com.rim.icrs.cassjmeterext.gui;

import java.awt.GridBagConstraints;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.apache.jmeter.testelement.TestElement;

import com.rim.icrs.cassjmeterext.gui.components.CompositeSerializerList;
import com.rim.icrs.cassjmeterext.sampler.EnhancedAbstractSampler;
import com.rim.icrs.cassjmeterext.sampler.EnhancedPutSampler;

public class EnhancedPut extends EnhancedAbstractGUI
{    
 	private static final long serialVersionUID = -2385526600604989215L;
	private static final String LABEL = "Cassandra Enhanced Put";    
	private JTextField CNAME;
    private JTextField VALUE;
    private JComboBox KSERIALIZER;
    private JComboBox CSERIALIZER;
    private JComboBox VSERIALIZER;
    private JCheckBox IS_COUNTER;
    
    private CompositeSerializerList COMPOSITE_KSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_CSERIALIZERS_LIST;
    private CompositeSerializerList COMPOSITE_VSERIALIZERS_LIST;
   
    @Override
    public void configure(TestElement element)
    {
        super.configure(element);
        CNAME.setText(element.getPropertyAsString(EnhancedPutSampler.COLUMN_NAME));
        VALUE.setText(element.getPropertyAsString(EnhancedPutSampler.VALUE));
        KSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedPutSampler.KEY_SERIALIZER_TYPE));
        CSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedPutSampler.COLUMN_SERIALIZER_TYPE));
        VSERIALIZER.setSelectedItem(element.getPropertyAsString(EnhancedPutSampler.VALUE_SERIALIZER_TYPE));
        IS_COUNTER.setSelected(element.getPropertyAsBoolean(EnhancedPutSampler.IS_COUNTER));
        COMPOSITE_KSERIALIZERS_LIST.setListModelElements(EnhancedPutSampler.getAsArray(element.getPropertyAsString(EnhancedPutSampler.COMPOSITE_KEY_SERIALIZER_TYPES), ","));
        COMPOSITE_CSERIALIZERS_LIST.setListModelElements(EnhancedPutSampler.getAsArray(element.getPropertyAsString(EnhancedPutSampler.COMPOSITE_COLUMN_SERIALIZER_TYPES), ","));
        COMPOSITE_VSERIALIZERS_LIST.setListModelElements(EnhancedPutSampler.getAsArray(element.getPropertyAsString(EnhancedPutSampler.COMPOSITE_VALUE_SERIALIZER_TYPES), ","));       
    }
    
    public TestElement createTestElement()
    {
    	EnhancedPutSampler sampler = new EnhancedPutSampler();
        modifyTestElement(sampler);
        sampler.setComment("test comment");     
        return sampler;
        
    }

    public void modifyTestElement(TestElement sampler)
    {
        super.configureTestElement(sampler);

        if (sampler instanceof EnhancedPutSampler)
        {
        	EnhancedPutSampler gSampler = (EnhancedPutSampler) sampler;
        	gSampler.setKSerializerType((String) KSERIALIZER.getSelectedItem());
            gSampler.setCSerializerType((String) CSERIALIZER.getSelectedItem());
            gSampler.setVSerializerType((String) VSERIALIZER.getSelectedItem());
            gSampler.setColumnName(CNAME.getText());
            gSampler.setValue(VALUE.getText());
            gSampler.setCounter(IS_COUNTER.isSelected());
                   
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
        VALUE.setText("${__Random(1,1000)}");
        KSERIALIZER.setSelectedItem("Key Serializer");
        CSERIALIZER.setSelectedItem("Column Serializer");
        VSERIALIZER.setSelectedItem("Value Serializer");
    }

    @Override
    public void init(JPanel mainPanel, GridBagConstraints labelConstraints, GridBagConstraints editConstraints)
    {
        addToPanel(mainPanel, labelConstraints, 0, 3, new JLabel("Column Name: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 3, CNAME = new JTextField());
        addToPanel(mainPanel, labelConstraints, 0, 4, new JLabel("Column Value: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 4, VALUE = new JTextField());
        
        addToPanel(mainPanel, labelConstraints, 0, 5, new JLabel("Key Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 5, KSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 6, COMPOSITE_KSERIALIZERS_LIST = new CompositeSerializerList());
        KSERIALIZER.addActionListener(COMPOSITE_KSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, 7, new JLabel("Column Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 7, CSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 8, COMPOSITE_CSERIALIZERS_LIST = new CompositeSerializerList());
        CSERIALIZER.addActionListener(COMPOSITE_CSERIALIZERS_LIST);
        
        
        addToPanel(mainPanel, labelConstraints, 0, 9, new JLabel("Value Serializer: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 9, VSERIALIZER = new JComboBox(EnhancedAbstractSampler.getSerializerNames().toArray()));
        addToPanel(mainPanel, editConstraints, 1, 10, COMPOSITE_VSERIALIZERS_LIST = new CompositeSerializerList());
        VSERIALIZER.addActionListener(COMPOSITE_VSERIALIZERS_LIST);
        
        addToPanel(mainPanel, labelConstraints, 0, 11, new JLabel("Counter: ", JLabel.RIGHT));
        addToPanel(mainPanel, editConstraints, 1, 11, IS_COUNTER = new JCheckBox());
    }

    @Override
    public String getLabel()
    {
        return LABEL;
    }    
}
