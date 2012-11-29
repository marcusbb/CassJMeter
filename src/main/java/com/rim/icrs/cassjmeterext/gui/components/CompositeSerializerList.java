package com.rim.icrs.cassjmeterext.gui.components;

import java.awt.BorderLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultListModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;

import com.netflix.jmeter.sampler.AbstractSampler;

public class CompositeSerializerList extends JPanel implements ActionListener {
	public static final String ADD_CMD = "CS_ADD";
	public static final String DEL_CMD = "CS_DEL";
	public static final String UP_CMD = "CS_UP";
	public static final String DOWN_CMD = "CS_DOWN";
	private static final long serialVersionUID = -8478497314878866160L;
	DefaultListModel listModel;
	JList list;
	JButton addButton;
	JButton removeButton;
	JButton upButton;
	JButton downButton;
	JComboBox serializerCombo;
	
	public CompositeSerializerList() {
		super(new BorderLayout());
		super.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));
		this.listModel =  new DefaultListModel();

		this.list = new JList(listModel);

        list.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        list.setSelectedIndex(0);
        
        list.setVisibleRowCount(5);
        JScrollPane listScrollPane = new JScrollPane(list);

        serializerCombo = new JComboBox(AbstractSampler.getSerializerNames().toArray());        

        upButton = new JButton("Up");
        upButton.setActionCommand(UP_CMD);
        upButton.addActionListener(new MoveListener());

        downButton = new JButton("Down");
        downButton.setActionCommand(DOWN_CMD);
        downButton.addActionListener(new MoveListener());
        
        addButton = new JButton("Add");
        addButton.setActionCommand(ADD_CMD);
        addButton.addActionListener(new AddListener());

        removeButton = new JButton("Remove");
        removeButton.setActionCommand(DEL_CMD);
        removeButton.addActionListener(new RemoveListener());
        

        JPanel controlPane = new JPanel();
        controlPane.setLayout(new BoxLayout(controlPane,
                                           BoxLayout.LINE_AXIS));
        controlPane.add(serializerCombo);
        controlPane.add(Box.createHorizontalStrut(5));
        controlPane.add(addButton);
        controlPane.add(Box.createHorizontalStrut(5));
        controlPane.add(new JSeparator(SwingConstants.VERTICAL));
        controlPane.add(Box.createHorizontalStrut(5));
        controlPane.add(removeButton);
        controlPane.add(upButton);
        controlPane.add(downButton);
        controlPane.setBorder(BorderFactory.createEmptyBorder(5,5,5,5));

        add(new JLabel("Select Serializers In Order for CompositeTypes", JLabel.CENTER), BorderLayout.PAGE_START);
        add(listScrollPane, BorderLayout.CENTER);
        add(controlPane, BorderLayout.PAGE_END);

	}	
	
    class AddListener implements ActionListener {

        public void actionPerformed(ActionEvent e) {
            String serializerName = (String) serializerCombo.getSelectedItem();

            if (serializerName.equals("")) {
                Toolkit.getDefaultToolkit().beep();
                serializerCombo.requestFocusInWindow();
                
                return;
            }

            listModel.addElement(serializerName);
            
            int newInd = listModel.size()-1;
        	list.requestFocusInWindow();
            list.setSelectedIndex(newInd);
            list.ensureIndexIsVisible(newInd);

   
        }

    }	

    class RemoveListener implements ActionListener {

        public void actionPerformed(ActionEvent e) {
        	
        	int selectedIndex = list.getSelectedIndex();        	
        	
        	if(selectedIndex >=0){
        		int newIndex = 0;
        		listModel.removeElementAt(selectedIndex);        		
        		if(selectedIndex < listModel.size()){
        			newIndex = selectedIndex;
        		}else if(selectedIndex > 0){
        			newIndex = selectedIndex - 1;
        		}  
        		
        		if(!listModel.isEmpty()){
        			list.requestFocusInWindow();
        			list.setSelectedIndex(newIndex);
        			list.ensureIndexIsVisible(newIndex);
        		}
        	}        	
        }
    }	

    class MoveListener implements ActionListener {

        public void actionPerformed(ActionEvent e) {
        	int selInd = list.getSelectedIndex();
        	if(selInd < 0){
        		return;
        		
        	}
        	      
        	int newInd = 0;
        	if(UP_CMD.equals(e.getActionCommand()) && selInd > 0){
        		newInd = selInd-1;        		        		
        	}else if(DOWN_CMD.equals(e.getActionCommand()) && selInd < (listModel.size()-1)){
        		newInd = selInd+1;        		
        	}else{
        		return;
        	}
        	
        	listModel.add(newInd, listModel.remove(selInd));
        	list.requestFocusInWindow();
            list.setSelectedIndex(newInd);
            list.ensureIndexIsVisible(newInd);
        }

    }

	@Override
	public void actionPerformed(ActionEvent e) {
		if ("comboBoxChanged".equals(e.getActionCommand())) {
			JComboBox parentSerializers = (JComboBox) e.getSource(); 
			String parentSerializer = (String)parentSerializers.getSelectedItem();
			this.setVisible("CompositeSerializer".equals(parentSerializer));
		}		
		
	}

	public void setListModelElements(String[] serializers){
		if(serializers == null){		
			return;
		}
		
		listModel.clear();
		for(String serializerName : serializers){			
			if (!"".equals(serializerName)) {
				listModel.addElement(serializerName);
			}			
		}
	}
		
	public String[] getListModelAsArray(){
		String[] array = new String[listModel.size()];
    	for(int i = 0; i < listModel.size(); i++){
    		array[i] = (String)listModel.elementAt(i);
    	}
    	
    	return array;
	}
	
	public DefaultListModel getListModel() {
		return listModel;
	}

	public void setListModel(DefaultListModel listModel) {
		this.listModel = listModel;
	}

	public JList getList() {
		return list;
	}

	public void setList(JList list) {
		this.list = list;
	}	
	
}
